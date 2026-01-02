const axios = require('axios');
const sql = require('mssql');
const mysql = require('mysql2/promise');
const oracledb = require('oracledb');
const createLogger = require('../config/logger');
const xml2js = require('xml2js');
const pool = require('../config/database');

class DataFetcher {
  constructor(config) {
    this.config = config;
    this.logger = createLogger(config.vendor_name || 'unknown');
  }

  async fetchData() {
    const sourceType = this.config.cac_jsonordb?.toLowerCase();
    
    try {
      // 🔹 STEP 1: Get max transaction date from database
      const maxDate = await this.getMaxTransactionDate();
      this.logger.info('Max transaction date retrieved', { maxDate });

      if (sourceType === 'json' || sourceType === 'api') {
        return await this.fetchFromAPI(maxDate);
      } else if (sourceType === 'xml' || sourceType === 'soap') {
        return await this.fetchFromXMLAPI(maxDate);
      } else if (sourceType === 'db' || sourceType === 'database') {
        return await this.fetchFromDatabase(maxDate);
      } else {
        throw new Error(`Unknown source type: ${sourceType}`);
      }
    } catch (error) {
      this.logger.error('Data fetch failed', { error: error.message, config: this.config.cac_config_id });
      throw error;
    }
  }

   /* =========================
     TOKEN CHECK
  ========================= */
  isTokenRequired() {
    return (
      this.config.cac_authtokenurl &&
      this.config.cac_authtokenfieldmapping &&
      this.config.cac_tokenhttp
    );
  }

    /* =========================
     GET AUTH TOKEN
  ========================= */
  async getAuthToken(maxDate) {
    this.logger.info('Fetching auth token');

    // const mapping = this.config.cac_authtokenfieldmapping;

     let headers = { ...(this.config.cac_authtokenfieldmapping.headers || {}) };
    let body = this.config.cac_authtokenfieldmapping.body || null;
let params = this.config.cac_authtokenfieldmapping.params || {};
console.log('Auth token request headers:', body);

    // 🔹 Replace placeholders
    const context = this.buildRuntimeContext(maxDate);
    headers = this.replacePlaceholders(headers, context);
     params  = this.replacePlaceholders(params, context);
    if (body) body = this.replacePlaceholders(body, context);


// console.log('Auth token request params:', params);

    const response = await axios({
      method: this.config.cac_tokenhttp || 'POST',
      url: this.config.cac_authtokenurl,
      headers,
      params,
      data: body,
      timeout: 20000
    });

    /**
     * Example response:
     * {
     *   success: 1,
     *   error: "",
     *   data: "4d42325a-85c9-452d-9242-8d259c19a7fc"
     * }
     */
// console.log('Auth token response:', response.data);
    const tokenPath = this.config.cac_tokenresponse || 'data';
    const token = this.getValueByPath(response.data, tokenPath);

    if (!token) {
      throw new Error(`Token not found at path: ${tokenPath}`);
    }

    this.logger.info('Auth token retrieved successfully');
    return token;
  }

  /**
   * Get the maximum transaction_date from raw_transactions for this configuration
   */
  async getMaxTransactionDate() {
    try {
      const query = `
        SELECT MAX(transaction_date) as max_date
        FROM raw_transactions
        WHERE brand_id = $1
          AND outlet_name = $2
          AND terminal = $3
          AND source_system = $4
      `;

      const values = [
        this.config.com_brand_id,
        this.config.cac_outlet_id,
        this.config.com_terminal,
        this.config.cac_pos_vendor
      ];

      const result = await pool.query(query, values);
      
      if (result.rows[0]?.max_date) {
        return result.rows[0].max_date;
      }

      // If no data exists, return yesterday as default
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      return yesterday.toISOString().slice(0, 10);

    } catch (error) {
      this.logger.error('Failed to get max transaction date', { error: error.message });
      // Return yesterday as fallback
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      return yesterday.toISOString().slice(0, 10);
    }
  }

  /*async fetchFromAPI(maxDate) {
    const { cac_api_url, cac_http_method, cac_field_mapping } = this.config;

    if (!cac_api_url) {
      throw new Error('API URL is missing');
    }

    if (!cac_field_mapping?.headers) {
      throw new Error('Headers not found in cac_field_mapping');
    }

    // 🔹 Resolve dynamic dates using maxDate
    const resolveDynamicDates = (headers, maxDate) => {
      const resolved = { ...headers };

      const today = new Date();
      const toDate = today.toISOString().slice(0, 10); // YYYY-MM-DD

      // Use maxDate as fromDate
      const fromDateStr = maxDate;

      if (resolved.datefrom === '{{FROM_DATE}}' || !resolved.datefrom) {
        resolved.datefrom = fromDateStr;
      }

      if (resolved.dateto === '{{TO_DATE}}' || !resolved.dateto) {
        resolved.dateto = toDate;
      }

      return resolved;
    };

    // 🔹 Prepare headers - remove problematic headers
    let headers = resolveDynamicDates(cac_field_mapping.headers, maxDate);
    
    // Remove headers that can cause issues with some APIs
    delete headers['User-Agent'];
    delete headers['Accept'];
    delete headers['Connection'];

    this.logger.info('Fetching data from API', {
      url: cac_api_url,
      method: cac_http_method || 'GET',
      dateRange: `${headers.datefrom} to ${headers.dateto}`,
      headers: Object.keys(headers) // DO NOT log values for security
    });

    let allData = [];
    let currentPage = Number(headers.currentpage) || 1;
    let totalPages = 1;

    try {
      do {
        headers.currentpage = String(currentPage);

        const response = await axios({
          method: cac_http_method || 'GET',
          url: cac_api_url,
          headers,
          timeout: 30000
        });

        const payload = response.data;

        // Expected API format:
        // { meta: { pagination: { totalPages } }, data: [...] }
        const pageData = payload?.data ?? payload;

        if (Array.isArray(pageData)) {
          allData.push(...pageData);
        } else if (pageData) {
          allData.push(pageData);
        }

        // ✅ FIXED: Correct pagination path
        totalPages = Number(payload?.meta?.pagination?.totalPages) || 1;
        
        this.logger.info('Page fetched successfully', {
          currentPage,
          totalPages,
          recordsInPage: Array.isArray(pageData) ? pageData.length : 1
        });

        currentPage++;

      } while (currentPage <= totalPages);

      this.logger.info('API fetch completed successfully', {
        totalRecords: allData.length,
        totalPages: totalPages,
        dateRange: `${headers.datefrom} to ${headers.dateto}`
      });

      return allData;

    } catch (error) {
      // ✅ IMPROVED: Better error logging
      this.logger.error('API fetch failed', {
        message: error.message,
        status: error.response?.status,
        statusText: error.response?.statusText,
        responseData: error.response?.data,
        url: cac_api_url,
        page: currentPage
      });
      throw error;
    }
  }*/
 async fetchFromAPI(maxDate) {
    const { cac_api_url, cac_http_method, cac_field_mapping,cac_xmlbody } = this.config;

    // if (!cac_api_url || !cac_field_mapping) {
    if (!cac_api_url ) {
      throw new Error('API URL or field mapping missing');
    }
let token = null;
 if (this.isTokenRequired()) {
       token = await this.getAuthToken(maxDate);
      // headers[this.config.cac_tokenhttp] = token;
    }

    let body = null
let headers = {};
let params = {};  
    

    // console.log('API request headers before placeholder replacement:', cac_xmlbody);
    if (cac_xmlbody) {
        body = cac_xmlbody;
    } 
    else {
     body = cac_field_mapping.body || null;
      headers = { ...(cac_field_mapping.headers || {}) };
      params = cac_field_mapping.params || {};
        }
    
// console.log('API request body before placeholder replacement:', body);

    // 🔹 Replace placeholders
    const context = this.buildRuntimeContext(maxDate);
    if (token) {
  context.Authorization = `${token}`;
}
   
      headers = this.replacePlaceholders(headers, context);
    
// console.log('API request headers:', headers);
     params  = this.replacePlaceholders(params, context);
    if (body) body = this.replacePlaceholders(body, context);

    // 🔹 Token logic (ONLY if configured)
   

   /* this.logger.info('Calling API', {
      url: cac_api_url,
      method: cac_http_method || 'POST',
      auth: this.isTokenRequired() ? 'ENABLED' : 'DISABLED',
      headers: Object.keys(headers) ,
      params: Object.keys(params), 
      bodyKeys: bodyKeys,
    });*/

    


    const response = await axios({
      method: cac_http_method || 'POST',
      url: cac_api_url,
      headers,
      params,
      data: body,
      timeout: 30000
    });

    // console.log('API response status:', response.data);
    return response.data;
  }

  replacePlaceholders(obj, context) {
    const str = JSON.stringify(obj);
    const replaced = str.replace(/{{(.*?)}}/g, (_, key) => {
      return context[key] ?? '';
    });
    return JSON.parse(replaced);
  }

  
  /* =========================
     RUNTIME CONTEXT
  ========================= */
  buildRuntimeContext(maxDate) {
    const today = new Date();

     const context = {
    FROM_DATE: maxDate,
    TO_DATE: today.toISOString().slice(0, 10),
    TRANS_DATE: today.toISOString().slice(0, 10)
      .split('-')
      .reverse()
      .join('/'),
    LOCATION_CODE: this.config.cac_outlet_id
  };

  // 🔥 GIVA ONLY → epoch millis
  if (this.isGivaVendor()) {
    context.FROM_EPOCH = new Date(maxDate).getTime();
    context.TO_EPOCH = today.getTime();
  }

  return context;
  }

  isGivaVendor() {
  return this.config.cac_customer_id === 'VVC00046';
}


splitTransactionDateTime(timestamp) {
  if (!timestamp) {
    return {
      transaction_date: null,
      transaction_time: null
    };
  }

  // Convert "2025-12-31 06:05:31+05:30" → ISO
  const isoTs = timestamp.includes('T')
    ? timestamp
    : timestamp.replace(' ', 'T');

  const d = new Date(isoTs);

  if (isNaN(d.getTime())) {
    this.logger.warn('Invalid transaction timestamp', { timestamp });
    return {
      transaction_date: null,
      transaction_time: null
    };
  }

  return {
    transaction_date: d.toISOString().slice(0, 10), // YYYY-MM-DD
    transaction_time: d.toTimeString().slice(0, 8)  // HH:mm:ss
  };
}


  /* =========================
     JSON PATH RESOLVER
  ========================= */
  getValueByPath(obj, path) {
    return path.split('.').reduce((acc, key) => {
      return acc && acc[key] !== undefined ? acc[key] : null;
    }, obj);
  }


  async fetchFromXMLAPI(maxDate) {
    const { cac_api_url, cac_http_method, cac_auth_type, cac_auth_header_key, cac_auth_header_value } = this.config;
    
    this.logger.info('Fetching XML/SOAP data from API', { 
      url: cac_api_url,
      fromDate: maxDate 
    });

    const headers = {
      'Content-Type': 'text/xml'
    };
    
    if (cac_auth_header_key && cac_auth_header_value) {
      headers[cac_auth_header_key] = cac_auth_header_value;
    }

    const response = await axios({
      method: cac_http_method || 'POST',
      url: cac_api_url,
      headers,
      timeout: 30000,
      responseType: 'text'
    });

    // Parse XML to JSON
    const parser = new xml2js.Parser({ explicitArray: false, mergeAttrs: true });
    const result = await parser.parseStringPromise(response.data);

    this.logger.info('XML/SOAP data fetched and parsed successfully');
    return result;
  }

  async fetchFromDatabase(maxDate) {
    const dbType = this.detectDatabaseType();
    
    this.logger.info('Fetching data from database', { 
      dbType, 
      host: this.config.cac_db_host,
      fromDate: maxDate 
    });

    switch (dbType) {
      case 'mssql':
        return await this.fetchFromMSSQL(maxDate);
      case 'mysql':
        return await this.fetchFromMySQL(maxDate);
      case 'oracle':
        return await this.fetchFromOracle(maxDate);
      default:
        throw new Error(`Unsupported database type: ${dbType}`);
    }
  }

  detectDatabaseType() {
    const port = this.config.cac_db_port;
    if (port === 1433) return 'mssql';
    if (port === 3306) return 'mysql';
    if (port === 1521) return 'oracle';
    
    // Fallback to vendor name or default
    const vendor = this.config.cac_pos_vendor?.toLowerCase();
    if (vendor?.includes('sql')) return 'mssql';
    if (vendor?.includes('mysql')) return 'mysql';
    if (vendor?.includes('oracle')) return 'oracle';
    
    return 'mssql'; // Default
  }

  async fetchFromMSSQL(maxDate) {
    const config = {
      server: this.config.cac_db_host,
      port: this.config.cac_db_port,
      database: this.config.cac_db_name,
      user: this.config.cac_db_username,
      password: this.config.cac_db_password,
      options: {
        encrypt: false,
        trustServerCertificate: true
      }
    };

    const pool = await sql.connect(config);
    const query = this.buildDatabaseQuery(maxDate);
    const result = await pool.request().query(query);
    await pool.close();

    return result.recordset;
  }

  async fetchFromMySQL(maxDate) {
    const connection = await mysql.createConnection({
      host: this.config.cac_db_host,
      port: this.config.cac_db_port,
      database: this.config.cac_db_name,
      user: this.config.cac_db_username,
      password: this.config.cac_db_password
    });

    const query = this.buildDatabaseQuery(maxDate);
    const [rows] = await connection.execute(query);
    await connection.end();

    return rows;
  }

  async fetchFromOracle(maxDate) {
    const connection = await oracledb.getConnection({
      user: this.config.cac_db_username,
      password: this.config.cac_db_password,
      connectString: `${this.config.cac_db_host}:${this.config.cac_db_port}/${this.config.cac_db_name}`
    });

    const query = this.buildDatabaseQuery(maxDate);
    const result = await connection.execute(query);
    await connection.close();

    return result.rows;
  }

  buildDatabaseQuery(maxDate) {
    // Build query based on field mapping
    const sampleJson = this.config.cac_sample_json;
    
    if (sampleJson && sampleJson.query) {
      return sampleJson.query;
    }

    const today = new Date().toISOString().slice(0, 10);

    // Query to get data from maxDate to current date
    return `
      SELECT * FROM transactions 
      WHERE CAST(transaction_date AS DATE) >= '${maxDate}'
        AND CAST(transaction_date AS DATE) <= '${today}'
      ORDER BY transaction_time DESC
    `;
  }
}

module.exports = DataFetcher;
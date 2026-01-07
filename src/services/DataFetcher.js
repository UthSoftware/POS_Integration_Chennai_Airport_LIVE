const axios = require('axios');
const sql = require('mssql');
const mysql = require('mysql2/promise');
const oracledb = require('oracledb');
const createLogger = require('../config/logger');
const xml2js = require('xml2js');
const {Pool } = require('pg');
const pool = require('../config/database');

class DataFetcher {
  constructor(config) {
    this.config = config;
    this.logger = createLogger(config.vendor_name || 'unknown');
  }

  /* ================= DATE FORMATTER ================= */
  formatDate(date, format) {
    if (!date) return null;

    const d = new Date(date);

    const dd = String(d.getDate()).padStart(2, '0');
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const yyyy = d.getFullYear();
    const yy = String(yyyy).slice(-2);

    const monthNames = [
      'JAN','FEB','MAR','APR','MAY','JUN',
      'JUL','AUG','SEP','OCT','NOV','DEC'
    ];
    const mmm = monthNames[d.getMonth()];

    switch (format) {
      case 'DD-MMM-YY':
        return `${dd}-${mmm}-${yy}`;     // 10-DEC-25
      case 'DD/MM/YYYY':
        return `${dd}/${mm}/${yyyy}`;    // 10/12/2025
      case 'YYYY-MM-DD':
      default:
        return `${yyyy}-${mm}-${dd}`;    // 2025-12-10
    }
  }
  
  async fetchData() {
    const sourceType = this.config.cac_jsonordb?.toLowerCase();
    
    try {
      // 🔹 STEP 1: Get max transaction date from database
      console.log('Fetching max transaction date for config:', this.config.dateformat);
      const maxDate = await this.getMaxTransactionDate(this.config.dateformat || 'YYYY-MM-DD');
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
  async getMaxTransactionDate(dateFormat = 'YYYY-MM-DD') {
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
      
      // if (result.rows[0]?.max_date) {
        // return result.rows[0].max_date;
      // }

      // If no data exists, return yesterday as default
      // const yesterday = new Date();
      // yesterday.setDate(yesterday.getDate() - 1);
      // return yesterday.toISOString().slice(0, 10);

      let date;

    if (result.rows[0]?.max_date) {
      date = result.rows[0].max_date;
    } else {
      // Default → yesterday
      date = new Date();
      date.setDate(date.getDate() - 1);
    }
console.log('Max transaction date from DB:', dateFormat);
    return this.formatDate(date, dateFormat);

    } catch (error) {
      this.logger.error('Failed to get max transaction date', { error: error.message });
      // Return yesterday as fallback
      const yesterday = new Date();
      yesterday.setDate(yesterday.getDate() - 1);
      return yesterday.toISOString().slice(0, 10);
    }
  }

  
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
    console.log('Runtime context for API call:', context);
    if (token) {
  context.Authorization = `${token}`;
}
   
      headers = this.replacePlaceholders(headers, context);
    
// console.log('API request headers:', headers);
     params  = this.replacePlaceholders(params, context);
    if (body) body = this.replacePlaceholders(body, context);

    // 🔹 Token logic (ONLY if configured)
   
console.log('API request headers:', body);
   /* this.logger.info('Calling API', {
      url: cac_api_url,
      method: cac_http_method || 'POST',
      auth: this.isTokenRequired() ? 'ENABLED' : 'DISABLED',
      headers: Object.keys(headers) ,
      params: Object.keys(params), 
      bodyKeys: bodyKeys,
    });*/

    
// console.log('API request body after placeholder replacement:', body);

    const response = await axios({
      method: cac_http_method || 'POST',
      url: cac_api_url,
      headers,
      params,
      data: body,
      timeout: 30000
    });

    console.log(
  'API response status:',
  JSON.stringify(response.data, null, 2)
);

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
    TO_DATE: this.formatDate(today, this.config.dateformat || 'YYYY-MM-DD'),
    TRANS_DATE: this.formatDate(today, this.config.dateformat || 'YYYY-MM-DD'),
    // TRANS_DATE: today.toISOString().slice(0, 10)
      // .split('-')
      // .reverse()
      // .join('/'),
      
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
    const { cac_api_url, cac_http_method, cac_auth_type, cac_auth_header_key,cac_xmlbody } = this.config;
    
    this.logger.info('Fetching XML/SOAP data from API', { 
      url: cac_api_url,
      fromDate: maxDate 
    });

    // const headers = {
      // 'Content-Type': 'text/xml'
    // };

    
    let body = null
let headers = {};
let params = {};  
    
     // console.log('API request headers before placeholder replacement:', cac_xmlbody);
    if (cac_xmlbody) {
        body = cac_xmlbody;
    } 
  
    // 🔹 Replace placeholders
    const context = this.buildRuntimeContext(maxDate);  
      headers = this.replacePlaceholders(headers, context);
    

     params  = this.replacePlaceholders(params, context);
    if (body) body = this.replacePlaceholders(body, context);

   
// console.log('API request headers:', body);
   

    const response = await axios({
      method: cac_http_method || 'POST',
      url: cac_api_url,
      headers,
      params,
      data: body,
      
      timeout: 30000,
      responseType: 'text'
    });
// console.log('Parsed XML/SOAP response:', JSON.stringify(response.data, null, 2));
    // Parse XML to JSON
    const parser = new xml2js.Parser({ explicitArray: false, mergeAttrs: true });
    const result = await parser.parseStringPromise(response.data);

    this.logger.info('XML/SOAP data fetched and parsed successfully');
    // console.log('Parsed XML/SOAP response:', JSON.stringify(result, null, 2));
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
      case 'pgsql':
        return await this.fetchFromPostgres(maxDate);  
      default:
        throw new Error(`Unsupported database type: ${dbType}`);
    }
  }

  detectDatabaseType() {
    const dbtype = this.config.cac_dbtype;
    if (dbtype === 'mssql') return 'mssql';
    if (dbtype === 'mysql') return 'mysql';
    if (dbtype === 'oracle') return 'oracle';
    if (dbtype === 'pgsql') return 'pgsql';

    // Fallback to vendor name or default
    // const vendor = this.config.cac_pos_vendor?.toLowerCase();
    // if (vendor?.includes('sql')) return 'mssql';
    // if (vendor?.includes('mysql')) return 'mysql';
    // if (vendor?.includes('oracle')) return 'oracle';
    
    // return 'mssql'; // Default
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

  async fetchFromPostgres(maxDate) {
  const pool = new Pool({
    user: this.config.cac_db_username,
    password: this.config.cac_db_password,
    host: this.config.cac_db_host,
    port: this.config.cac_db_port,
    database: this.config.cac_db_name
  });
// console.log('Postgres connection config:', {
    // user: this.config.cac_db_username,
    // host: this.config.cac_db_host,
  // });
  const query = this.config.cac_sql_text;
  const result = await pool.query(query);
// console.log('Postgres query result:', result.rows);
  await pool.end(); // close pool (optional if global)

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
const axios = require('axios');
const sql = require('mssql');
const mysql = require('mysql2/promise');
const oracledb = require('oracledb');
const createLogger = require('../config/logger');
const xml2js = require('xml2js');
const { Pool } = require('pg');
const pool = require('../config/database');
const { parseSoapResponse } = require('../services/xmlParser');

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

// ⭐ ADD TIME PARTS
    const HH = String(d.getHours()).padStart(2, '0');
    const min = String(d.getMinutes()).padStart(2, '0');
    const ss = String(d.getSeconds()).padStart(2, '0');
  
    
    const monthNames = [
      'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN',
      'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'
    ];
    const mmm = monthNames[d.getMonth()];

    switch (format) {
      case 'DD-MMM-YY':
        return `${dd}-${mmm}-${yy}`;     // 10-DEC-25
      case 'DD-MMM-YYYY':
        return `${dd}-${mmm}-${yyyy}`;   // 10-DEC-2025
      case 'DD/MM/YYYY':
        return `${dd}/${mm}/${yyyy}`;    // 10/12/2025
      case 'YYYY-DD-MM':
        return `${yyyy}-${dd}-${mm}`;
      case 'MM/DD/YYYY':
        return `${mm}/${dd}/${yyyy}`;
      case 'YYYY/DD/MM':
        return `${yyyy}/${dd}/${mm}`;  // 2025/12/10

       // ⭐ NEW CASE FOR UNIQUE COLORS
      case 'YYYY-MM-DD HH:mm:ss':
        return `${yyyy}-${mm}-${dd} ${HH}:${min}:${ss}`;  
      case 'YYYY-MM-DD':
      default:
        return `${yyyy}-${mm}-${dd}`;    // 2025-12-10
    }
  }

  /* =========================
   VIDVEDA SOAP CALL
========================= */
async getMyJOSoap(fromDate, toDate) {
  const url = "https://wizapp.in/mirrorservice/Service.asmx";

  const soapBody = `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetSaleDataForMall_V1 xmlns="http://tempuri.org/">
      <cGrpCode>BJ@WZBJ000001</cGrpCode>
      <cUserId>ONLINE</cUserId>
      <cPassword>123</cPassword>
      <cStoreCode>${this.config.cac_db_name}</cStoreCode>
      <cFromDt>${fromDate}</cFromDt>
      <ToDt>${toDate}</ToDt>
    </GetSaleDataForMall_V1>
  </soap:Body>
</soap:Envelope>`;

  const headers = {
    "Content-Type": "text/xml",
    "SOAPAction": "http://tempuri.org/GetSaleDataForMall_V1",
    "Accept": "*/*" // IMPORTANT: allow JSON or XML
  };

  try {
    const response = await axios.post(url, soapBody, {
      headers,
      timeout: 15000,
      transformResponse: res => res // prevent axios auto parsing
    });

    const raw = response.data.trim();

    let tRecordJson;

    // 🧠 CASE 1: Server returned JSON directly
    if (raw.startsWith("{")) {
      tRecordJson = JSON.parse(raw);
    }
    // 🧠 CASE 2: Server returned SOAP XML
    else if (raw.startsWith("<")) {
      const parsed = await xml2js.parseStringPromise(raw, {
        explicitArray: false
      });

      const body =
        parsed["soap:Envelope"]["soap:Body"]["GetSaleDataForMall_V1Response"];

      const result = body["GetSaleDataForMall_V1Result"];
      tRecordJson = JSON.parse(result);
    }
    else {
      throw new Error("Unknown response format from POS API");
    }

    // 🧾 Convert header-row array → objects
    const headerRow = tRecordJson.tRecord[0];
    const rows = tRecordJson.tRecord.slice(1);

    const sales = rows.map(row => {
      const obj = {};
      headerRow.forEach((key, index) => {
        obj[key.replace(/\s+/g, "_")] = row[index];
      });
      return obj;
    });

    return sales;

  } catch (err) {
    console.error("❌ POS API Error");
    console.error("Message:", err.message);
    throw err;
  }
}





  async getAllSegments(fromDate, toDate) {
    // console.log('Fetching all segments from SOAP API for dates:', fromDate, toDate);
    const [
      transactionSegment,
      itemSegment,
      paymentSegment
    ] = await Promise.all([
      this.callSoapMethod('TransactionSegment', fromDate, toDate),
      this.callSoapMethod('ItemSegment', fromDate, toDate),
      this.callSoapMethod('PaymentSegment', fromDate, toDate)
    ]);

    return this.groupByReceiptNoFromSoap({
      transactionSegment,
      itemSegment,
      paymentSegment
    });
  }


  /* =========================
     SOAP CALLER
  ========================= */
  async callSoapMethod(methodName, fromDate, toDate) {
    try {
      
       const soapEnvelope = this.buildSoapEnvelope(methodName, fromDate, toDate);
       

      const response = await axios.post(
        this.config.cac_api_url,
        // 'https://hidesign.eshopaid.com/ADSR/eShopaidservices.asmx',
        soapEnvelope,
        {
          headers: {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': this.config.cac_soap_action || ''
          },
          timeout: this.config.cac_soap_timeout || 30000,
          responseType: 'text'
        }
      );
      // console.log(`SOAP response for ${methodName}:`, response.data);

      // Optional logging
      if (this.config.cac_log_soap) {
        const fs = require('fs');
        fs.appendFileSync(
          'logs/soap.log',
          `===== ${methodName} =====\n${response.data}\n\n`
        );
      }

      // Parse SOAP XML → JSON
      const parser = new xml2js.Parser({ explicitArray: false });
      const parsed = await parser.parseStringPromise(response.data);

return await parseSoapResponse(response.data);
      
     
//  console.log('Parsed SOAP response for method', methodName, JSON.stringify(parsed, null, 2));

      

    } catch (error) {
      throw new Error(`[SOAP:${methodName}] ${error.response?.data || error.message}`);
    }
  }

  groupByReceiptNoFromSoap(data) {
    const { transactions, items, payments } =
      this.extractArraysFromSoapResponse(data);

    const grouped = {};

    // 🔹 Transactions (base)
    for (const tx of transactions) {
      const receipt = tx.RECEIPT_NO;
      if (!receipt) continue;

      grouped[receipt] = {
        receipt_no: receipt,
        transaction: tx,
        items: [],
        payments: []
      };
    }

    // 🔹 Items
    for (const item of items) {
      const receipt = item.RECEIPT_NO;
      if (!receipt) continue;

      if (!grouped[receipt]) {
        grouped[receipt] = {
          receipt_no: receipt,
          transaction: null,
          items: [],
          payments: []
        };
      }

      grouped[receipt].items.push(item);
    }

    // 🔹 Payments
    for (const pay of payments) {
      const receipt = pay.RECEIPT_NO;
      if (!receipt) continue;

      if (!grouped[receipt]) {
        grouped[receipt] = {
          receipt_no: receipt,
          transaction: null,
          items: [],
          payments: []
        };
      }

      grouped[receipt].payments.push(pay);
    }
    // console.log('Grouped SOAP data by receipt no:', Object.values(grouped));
    console.log(
  'Grouped SOAP data by receipt no:\n',
  JSON.stringify(Object.values(grouped), null, 2)
);
    return Object.values(grouped);
  }


  extractArraysFromSoapResponse(data) {
    const transactions =
      data?.transactionSegment
        ?.['soap:Envelope']
        ?.['soap:Body']
        ?.GetResponseAsDataSetResponse
        ?.GetResponseAsDataSetResult
        ?.['diffgr:diffgram']
        ?.eShopaidTransactionSegment
        ?.TransactionSegment || [];

    const items =
      data?.itemSegment
        ?.['soap:Envelope']
        ?.['soap:Body']
        ?.GetResponseAsDataSetResponse
        ?.GetResponseAsDataSetResult
        ?.['diffgr:diffgram']
        ?.eShopaidItemSegment
        ?.ItemSegment || [];

    const payments =
      data?.paymentSegment
        ?.['soap:Envelope']
        ?.['soap:Body']
        ?.GetResponseAsDataSetResponse
        ?.GetResponseAsDataSetResult
        ?.['diffgr:diffgram']
        ?.NewDataSet
        ?.Table || [];

    return {
      transactions: Array.isArray(transactions) ? transactions : [transactions],
      items: Array.isArray(items) ? items : [items],
      payments: Array.isArray(payments) ? payments : [payments]
    };
  }

  buildSoapEnvelope(methodName, fromDate, toDate, optionalData = '') {
    return `<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope
  xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
  xmlns:esh="http://eshopaid.in">

  

  <soapenv:Header>
    <esh:eShopaidSoapHeader>
      <esh:UserName>${this.config.cac_db_username}</esh:UserName>
      <esh:Password>${this.config.cac_db_password}</esh:Password>
      
      <esh:MethodName>${methodName}</esh:MethodName>
      <esh:FromDate>${fromDate}</esh:FromDate>
      <esh:ToDate>${toDate}</esh:ToDate>
      <esh:OptionalData>${optionalData}</esh:OptionalData>
    </esh:eShopaidSoapHeader>
  </soapenv:Header>

  <soapenv:Body>
    <esh:GetResponseAsDataSet />
  </soapenv:Body>

</soapenv:Envelope>`;
  }


 buildSoapEnvelopemyjo(methodName, fromDate, toDate, optionalData = '') {
    return `<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema"
               xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetSaleDataForMall_V1 xmlns="http://tempuri.org/">
      <cGrpCode>BJ@WZBJ000001</cGrpCode>
      <cUserId>ONLINE</cUserId>
      <cPassword>123</cPassword>
      <cStoreCode>M3</cStoreCode>
      <cFromDt>2026-02-01</cFromDt>
      <cToDt>2026-02-07</cToDt>
    </GetSaleDataForMall_V1>
  </soap:Body>
</soap:Envelope>`;
  } 


  async fetchThreeApisInLoop(maxDate) {
    if (!Array.isArray(this.config.cac_multiple_apis)) {
      throw new Error('cac_multiple_apis must be an array');
    }

    const combinedResponse = {
      success: true,
      fromDate: maxDate,
      toDate: this.buildRuntimeContext(maxDate).TO_DATE,
      data: {},
      errors: []
    };

    for (const apiConfig of this.config.cac_multiple_apis) {
      try {
        // 🔹 Create isolated fetcher per API
        const fetcher = new DataFetcher({
          ...this.config,
          ...apiConfig
        });

        const response = await fetcher.fetchFromAPI(maxDate);

        combinedResponse.data[apiConfig.api_name] = response;
      } catch (error) {
        this.logger.error('API fetch failed', {
          api: apiConfig.api_name,
          error: error.message
        });

        combinedResponse.errors.push({
          api: apiConfig.api_name,
          error: error.message
        });
      }
    }

    return combinedResponse;
  }

  async getCombinedDetails(Fromdate, Todate) {
    const { cac_api_url } = this.config;

    const baseUrl = cac_api_url
      .trim()
      .replace(/\/+$/, '');

    // this.logger.info('Fetching multi-API data', { baseUrl });
    // this.logger.info('params', {Fromdate, Todate});

    const results = { items: [], payments: [], transactions: [] };

    const callApi = async (path) =>
      axios.get(`${baseUrl}/${path}`, { params: { Fromdate, Todate } });

    try {
      results.items = (await callApi('ItemdetailsGet')).data;
    } catch (e) {
      this.logger.error('❌ ItemdetailsGet failed', e.response?.data || e.message);
    }

    try {
      results.payments = (await callApi('PaymentdetailsGet')).data;
    } catch (e) {
      this.logger.error('❌ PaymentdetailsGet failed', e.response?.data || e.message);
    }

    try {
      results.transactions = (await callApi('TransactiondetailsGet')).data;
    } catch (e) {
      this.logger.error('❌ TransactiondetailsGet failed', e.response?.data || e.message);
    }

    return this.groupByReceiptmultiapi(
      results.items,
      results.payments,
      results.transactions
    );
  }
  


  groupByReceiptmultiapi(items, payments, transactions) {
    const grouped = {};

    // Transactions (1 per receipt usually)
    transactions.forEach(txn => {
      const rcpt = txn.RCPT_NUM;
      if (!grouped[rcpt]) {
        grouped[rcpt] = {
          RCPT_NUM: rcpt,
          transaction: null,
          items: [],
          payments: []
        };
      }
      grouped[rcpt].transaction = txn;
    });

    // Items (many per receipt)
    items.forEach(item => {
      const rcpt = item.RCPT_NUM;
      if (!grouped[rcpt]) {
        grouped[rcpt] = {
          RCPT_NUM: rcpt,
          transaction: null,
          items: [],
          payments: []
        };
      }
      grouped[rcpt].items.push(item);
    });

    // Payments (many per receipt)
    payments.forEach(pay => {
      const rcpt = pay.RCPT_NUM;
      if (!grouped[rcpt]) {
        grouped[rcpt] = {
          RCPT_NUM: rcpt,
          transaction: null,
          items: [],
          payments: []
        };
      }
      grouped[rcpt].payments.push(pay);
    });

    // console.log('Grouped multi-API data by receipt no:', Object.values(grouped));
    return Object.values(grouped);
  }



  async fetchData() {
    const sourceType = this.config.cac_jsonordb?.toLowerCase();

    try {
      // 🔹 STEP 1: Get max transaction date from database
      // console.log('Fetching max transaction date for config:', this.config.dateformat);
      const maxDate = await this.getMaxTransactionDate(this.config.dateformat || 'YYYY-MM-DD');
      // this.logger.info('Max transaction date retrieved', { maxDate });


      if (sourceType === 'json' || sourceType === 'api') {
        return await this.fetchFromAPI(maxDate);


      } else if (sourceType == 'multiapi') {//KADASAM CUSTOMER..
        //  return await this.getCombinedDetails(maxDate, this.buildRuntimeContext(maxDate).TO_DATE);
        // console.log('Fetching multi-API data for dates:', maxDate, this.buildRuntimeContext(maxDate).TO_DATE);
        return await this.getCombinedDetails((this.buildRuntimeContext(maxDate).FROM_DATE), (this.buildRuntimeContext(maxDate).TO_DATE));
      }
      else if (sourceType == 'multiapizoho') {//KADASAM CUSTOMER..
        //  return await this.getCombinedDetails(maxDate, this.buildRuntimeContext(maxDate).TO_DATE);
        // console.log('Fetching multi-API data for dates:', maxDate, this.buildRuntimeContext(maxDate).TO_DATE);
        return await this.getCombinedDetailszoho((this.buildRuntimeContext(maxDate).FROM_DATE), (this.buildRuntimeContext(maxDate).TO_DATE));
      }
      else if (sourceType === 'soap') {
        if (this.config.cac_db_name ){
           return await this.getMyJOSoap((this.buildRuntimeContext(maxDate).FROM_DATE), (this.buildRuntimeContext(maxDate).TO_DATE));
         }
         else{
        return await this.getAllSegments((this.buildRuntimeContext(maxDate).FROM_DATE), this.buildRuntimeContext(maxDate).TO_DATE);
        // return await this.getAllSegments('2026-02-14', this.buildRuntimeContext(maxDate).TO_DATE);
        // }
      }
    }
      else if (sourceType === 'xml') {
        return await this.fetchFromXMLAPI(maxDate);
        // } else if (sourceType === 'db' || sourceType === 'database') {
        // return await this.fetchFromDatabase(maxDate);
      } else {
        throw new Error(`Unknown source type: ${sourceType}`);
      }
    } catch (error) {
      this.logger.error('Data fetch failed', { error: error.message, config: this.config.cac_config_id });
      throw error;
    }
  }

  groupByReceiptmultiapizoho(items, payments, transactions,billnoKey) {
  const grouped = {};

  // 🔹 Transactions
  transactions.forEach(txn => {
    const billNo = txn[billnoKey];
    if (!billNo) return;

    if (!grouped[billNo]) {
      grouped[billNo] = {
        billNumber: billNo,
        transaction: null,
        items: [],
        payments: []
      };
    }
    // console.log('Mapping transaction for bill no:', billNo, txn);
    grouped[billNo].transaction = txn;
  });

  // 🔹 Items
  items.forEach(item => {
    const billNo = item.billNumber;
    if (!billNo) return;

    if (!grouped[billNo]) {
      grouped[billNo] = {
        billNumber: billNo,
        transaction: null,
        items: [],
        payments: []
      };
    }
    grouped[billNo].items.push(item);
  });

  // 🔹 Payments
  payments.forEach(pay => {
    const billNo = pay.billNumber;
    if (!billNo) return;

    if (!grouped[billNo]) {
      grouped[billNo] = {
        billNumber: billNo,
        transaction: null,
        items: [],
        payments: []
      };
    }
    grouped[billNo].payments.push(pay);
  });

  // console.log('Grouped multi-API Zoho data by bill number:', Object.values(grouped));
  return Object.values(grouped);
}


 async getCombinedDetailszoho(Fromdate, Todate) {

  let apis = this.config.cac_multiple_apis;
  if (typeof apis === 'string') {
    apis = JSON.parse(apis).cac_multiple_apis;
  }

  const baseUrl = this.config.cac_api_url
    .replace(/\s+/g, '')   // 🔥 CRITICAL
    .replace(/\/+$/, '');

  const results = {
    transactions: [],
    items: [],
    payments: []
  };
// console.log('APIs to call:', apis);
  for (const api of apis) {
    try {
      const url = `${baseUrl}/${api.path}`;

      this.logger.info('Calling Zoho API', {
        url,
        params: {
          dFrmDate: this.formatDate(Fromdate, 'DD-MMM-YYYY'),
          dToDate: this.formatDate(Todate, 'DD-MMM-YYYY'),
          publickey: api.public_key
        }
      });

      const res = await axios.get(url, {
        params: {
          // dFrmDate: this.formatDate(Fromdate, 'DD-MMM-YYYY'), // 🔥 Zoho format
          // dToDate: this.formatDate(Todate, 'DD-MMM-YYYY'),
          dFrmDate: this.formatDate(Fromdate, 'DD-MMM-YYYY'),
          dToDate: this.formatDate(Todate, 'DD-MMM-YYYY'),
          publickey: api.public_key
        }
      });

      results[api.api_name] = this.normalizeZohoResponse(res.data);
      //  console.log(
      // `Zoho ${api.api_name} records count:`,
      // results[api.api_name].length
    // );

    } catch (err) {
      this.logger.error(`❌ ${api.api_name} failed`, {
        error: err.response?.data || err.message
      });
    }
  }

  return this.groupByReceiptmultiapizoho(
    results.items,
    results.payments,
    results.transactions,'billNumber'
  );
}

  normalizeZohoResponse(res) {
  if (!res) return [];

  // ✅ ZOHO CREATOR FORMAT
  if (Array.isArray(res?.result?.Data)) {
    return res.result.Data;
  }

  // fallback safety
  if (Array.isArray(res)) return res;

  return [];
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
    const bodyType = (this.config.cac_authtoken_body_type || 'json').toLowerCase();

    let data;

    let headers = { ...(this.config.cac_authtokenfieldmapping.headers || {}) };
    let rawBody = this.config.cac_authtokenfieldmapping.body || null;
    let params = this.config.cac_authtokenfieldmapping.params || {};


    if (bodyType === 'x-www-form-urlencoded') {
      headers['Content-Type'] = 'application/x-www-form-urlencoded';

      const form = new URLSearchParams();
      Object.entries(rawBody).forEach(([k, v]) => {
        if (v !== undefined && v !== null) {
          form.append(k, v);
        }
      });

      data = form.toString();
    }
    else {
      // DEFAULT → JSON
      headers['Content-Type'] = 'application/json';
      data = rawBody;
    }

    // console.log('Auth token request headers:', rawBody);

    // 🔹 Replace placeholders
    const context = this.buildRuntimeContext(maxDate);
    headers = this.replacePlaceholders(headers, context);
    params = this.replacePlaceholders(params, context);
    if (rawBody) rawBody = this.replacePlaceholders(rawBody, context);


    // console.log('Auth token request params:', params);

    const response = await axios({
      method: this.config.cac_tokenhttp || 'POST',
      url: this.config.cac_authtokenurl,
      headers,
      params,
      data: data,
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
      // console.log('Max transaction date from DB:', dateFormat);
      return new Date(date);
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
    const { cac_api_url, cac_http_method, cac_field_mapping, cac_xmlbody } = this.config;

    // if (!cac_api_url || !cac_field_mapping) {
    if (!cac_api_url) {
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
    // console.log('Runtime context for API call:', context);
    if (token) {
      context.Authorization = `${token}`;
    }

    headers = this.replacePlaceholders(headers, context);

    // console.log('API request headers:', headers);
    params = this.replacePlaceholders(params, context);
    if (body) body = this.replacePlaceholders(body, context);

    // 🔹 Token logic (ONLY if configured)

    console.log('API request headers:', body);
    //  this.logger.info('Calling API', {
      //  url: cac_api_url,

      //  method: cac_http_method || 'POST',
       
      //  headers: Object.keys(headers) ,
      //  params: Object.keys(params)
       
    //  });


    console.log('API request body after placeholder replacement:', params);
    console.log('API request body after placeholder replacement:', headers);

    // console.log('API request body after placeholder replacement:',  cac_http_method || 'POST', body);
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

    const contentType = response.headers['content-type'] || '';

if (typeof response.data === 'string' && contentType.includes('xml')) {
  const parser = new xml2js.Parser({ explicitArray: false });
  const parsedXml = await parser.parseStringPromise(response.data);
  console.log('Parsed XML API response:', JSON.stringify(parsedXml, null, 2));
  return parsedXml;
}

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
    const fromEpochMs = maxDate.getTime();
  const toEpochMs = today.getTime();
    // console.log('format date :',this.config.dateformat);  
    const context = {
      FROM_DATE: this.formatDate(maxDate, this.config.dateformat || 'YYYY-MM-DD'),
       // milliseconds (default)
    FROM_EPOCH: fromEpochMs,
    TO_EPOCH: toEpochMs,

      TO_DATE: this.formatDate(today, this.config.dateformat || 'YYYY-MM-DD'),
      TRANS_DATE: this.formatDate(today, this.config.dateformat || 'YYYY-MM-DD'),
      // TRANS_DATE: today.toISOString().slice(0, 10)
      // .split('-')
      // .reverse()
      // .join('/'),

     
 FROM_EPOCHCRAFT: Math.floor(fromEpochMs / 1000),
    TO_EPOCHCRAFT: Math.floor(toEpochMs / 1000) ,
      LOCATION_CODE: this.config.cac_outlet_id
    };
//  console.log('format date :',maxDate);
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
    const { cac_api_url, cac_http_method, cac_auth_type, cac_auth_header_key, cac_xmlbody } = this.config;

    this.logger.info('Fetching XML/SOAP data from API', {
      url: cac_api_url,
      fromDate: maxDate
    });




    let body = null
    let headers = {};
    let params = {};


    if (cac_xmlbody) {
      body = cac_xmlbody;
    }

    // 🔹 Replace placeholders
    const context = this.buildRuntimeContext(maxDate);
    headers = this.replacePlaceholders(headers, context);


    params = this.replacePlaceholders(params, context);
    if (body) body = this.replacePlaceholders(body, context);


console.log('Final XML API request body:', params);
console.log('Final XML API request headers:', headers);


    const response = await axios({
      method: cac_http_method || 'POST',
      url: cac_api_url,
      headers,
      params,
      data: body,

      timeout: 30000,
      responseType: 'text'
    });
    console.log('Parsed XML/SOAP response:', JSON.stringify(response.data, null, 2));
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
const { v4: uuidv4 } = require('uuid');
const createLogger = require('../config/logger');
const xml2js = require('xml2js');
const dayjs = require('dayjs');
const customParseFormat = require('dayjs/plugin/customParseFormat');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');

dayjs.extend(customParseFormat);
dayjs.extend(utc);
dayjs.extend(timezone);

class FieldMapper {
  constructor(config, fieldMappings) {
    this.config = config;
    this.fieldMappings = fieldMappings;
    this.logger = createLogger(config.vendor_name || 'FieldMapper');
    this.sourceType = config.cac_jsonordb?.toLowerCase();
  }

parseApiDateTime(dateTimeStr) {
    if (!dateTimeStr) return null;

    // Handles "01/01/2026 03:08:08 PM" (DD/MM/YYYY hh:mm:ss A)
    const dt = dayjs(dateTimeStr, 'DD/MM/YYYY hh:mm:ss A', true);

    if (!dt.isValid()) {
      this.logger.warn('Invalid API datetime', { dateTimeStr });
      return null;
    }

    // Convert to IST timezone explicitly (same as your DB)
    // return dt.tz('Asia/Kolkata').toDate(); // JS Date object
    return dt
    .tz('Asia/Kolkata')
    .format('YYYY-MM-DD HH:mm:ss');
  }


  /* ========================= PUBLIC ========================== */
  async mapTransactions(rawData) {
    // Parse XML if needed
    if ((this.sourceType === 'xml' || this.sourceType === 'soap') && typeof rawData === 'string') {
      rawData = await this.parseXml(rawData);
    }

    // const records = Array.isArray(rawData) ? rawData : [rawData];
    // const result = [];

     // 🔥 Get transaction row root dynamically
  const txMappings = this.getMappings('raw_transactions');
  // console.log(
  // 'Transaction Row Root:',
  // txMappings.map(m => m.pvfm_row_root_json_path)
// );
  const rowRoot = txMappings[0]?.pvfm_row_root_json_path||null;




  let records;
  if (rowRoot) {
    records = this.extractByJsonPath(rawData, rowRoot);
  } else {
    // If no row root → rawData itself is transaction list
    records = rawData;
  }
console.log(
  'TX ROOT RESULT:',
  this.extractByJsonPath(rawData, 'Response.Transactions.Transaction[*]')
);

  if (!records) {
    throw new Error('No transaction records found');
  }


  // const records = this.extractByJsonPath(rawData, rowRoot) || [];
  const result = [];

  const txArray = Array.isArray(records) ? records : [records];

    // for (const record of records) {
     for (const record of txArray) {
      try {
        const tx = this.mapSingleTransaction(record);

        // Map items and payments dynamically (either array or single object)
        tx.items = this.mapFlexibleTable(record, tx, 'raw_transaction_items', tx.transaction_id);
        tx.payments = this.mapFlexibleTable(record, tx, 'raw_payment', tx.transaction_id);

        // this.logger.info('Transaction mapped', {
          // transaction_id: tx.transaction_id,
          // items_count: tx.items.length,
          // payments_count: tx.payments.length
        // });

        result.push(tx);
      } catch (err) {
        this.logger.error('❌ Transaction mapping failed', {
    error: err.message
  });
            }
    }

    return result;
  }

  /* ========================= TRANSACTION ========================== */
  mapSingleTransaction(record) {
    const mappings = this.getMappings('raw_transactions');

    const tx = {
      transaction_id: uuidv4(),
      source_system: this.config.cac_pos_vendor,
      agent_id: this.config.cac_config_id,
      batch_id: uuidv4(),
      brand_id: this.config.com_brand_id,
      brand_name: this.config.brand_name,
      outlet_id: this.config.com_outlet_id,
      outlet_name: this.config.cac_outlet_id,
      terminal: this.config.com_terminal,
      gate: this.config.com_gate,
      // received_at: dayjs().tz('Asia/Kolkata').format('YYYY-MM-DD HH:mm:ss'),
      transaction_time: null,
      received_at:null,
      // transaction_time: dayjs().tz('Asia/Kolkata').format('YYYY-MM-DD HH:mm:ss'),
      
      transaction_type: 'SALE',
      gross_amount: 0,
      discount_amount: 0,
      tax_amount: 0,
      net_amount: 0,
      invoice_no: 0,
      com_vendorname: this.config.cac_pos_vendor,
      // meta: record
    };

    for (const m of mappings) {
      const value = this.applyMapping(record, m);



      if (value !== undefined && value !== null) tx[m.pvfm_source_field] = value;
    }
// console.log('Mapped transaction', { transaction_id: tx.transaction_id, mapped_fields: tx });

 
    return tx;
  }

  /* ========================= FLEXIBLE TABLE MAPPER ========================== */
  mapFlexibleTable(record, txMapped, tableName, transactionId) {
     const mappings = this.getMappings(tableName);
  if (!mappings.length) return [];

  const rowRoot = mappings[0]?.pvfm_row_root_json_path||null;

let rows;
  if (rowRoot) {
    // rows = this.extractByJsonPath(record, rowRoot);
     rows = rowRoot
    ? this.extractByJsonPath(record, rowRoot):[];

    // this.logger.info('RowRoot extraction result', {
  // tableName,
  // rowRoot,
  // isArray: Array.isArray(rows),
  // length: Array.isArray(rows) ? rows.length : 'N/A',
  // rows
// });
  } else {
    // If no row root → record itself is transaction list
    rows = record;
  }

  if (!rows) {
    // this.logger.warn('⚠️ No rows found after rowRoot resolution', {
    // tableName,
    // rowRoot,
    // transaction_id: transactionId
  // });
  rows = record;
  // return [];
  }

   
  // let rows = rowRoot
    // ? this.extractByJsonPath(record, rowRoot):[];
    

  if (!Array.isArray(rows)) rows = [rows];
  if (!rows.length) return [];

  return rows.map(row => {
      const mapped = {
        transaction_id: transactionId,
        brand_id: txMapped.brand_id,
        brand_name: txMapped.brand_name,
        outlet_id: txMapped.outlet_id,
        outlet_name: txMapped.outlet_name,
        terminal: txMapped.terminal,
        gate: txMapped.gate,
        transaction_time: txMapped.transaction_time,
        received_at: txMapped.received_at,
        invoice_no: txMapped.invoice_no
        // meta: row
      };

      // Table-specific default fields
      if (tableName === 'raw_transaction_items') {
        mapped.item_line_id = uuidv4();
        // mapped.item_id = null;
        // mapped.sku = null;
        // mapped.item_name = null;
        // mapped.category = null;
        // mapped.subcategory = null;
        // mapped.quantity = 0;
        // mapped.unit_price = 0;
        // mapped.line_total = 0;
        // mapped.line_discount = 0;
        // mapped.line_tax = 0;
        // mapped.void_flag = false;
      } else if (tableName === 'raw_payment') {
        mapped.payment_id = uuidv4();
        // mapped.payment_type = 'UNKNOWN';
        // mapped.amount = txMapped.net_amount || 0;
      }
//  if (tableName === 'raw_transaction_items') {
        // mapped.item_line_id = uuidv4();
      // }

      // if (tableName === 'raw_payment') {
        // mapped.payment_id = uuidv4();
      // }
      // Apply field mappings
      for (const m of mappings) {
        let value = this.applyMapping(row, m);
        if (value !== undefined && value !== null) {
          mapped[m.pvfm_source_field] = value;
        }
      }

  // console.log('Mapped row', {
    // table: tableName,
    // transaction_id: transactionId,
    // mapped_fields: mapped // log all mapped fields
  // });
      return mapped;
    });
  }

  /* ========================= HELPERS ========================== */
  getMappings(tableName) {
    return this.fieldMappings.filter(m => m.pvfm_tablename === tableName);
  }


   /* ---- Deepest array becomes row root ---- */
  getArrayRoot(mappings) {
    let deepestPath = null;
    let maxDepth = -1;

    for (const m of mappings) {
      if (!m.pvfm_json_path) continue;

      const parts = m.pvfm_json_path.split('.');
      const arrayIndexes = parts
        .map((p, i) => (p.endsWith('[*]') ? i : -1))
        .filter(i => i !== -1);

      if (arrayIndexes.length > maxDepth) {
        maxDepth = arrayIndexes.length;
        const lastIndex = arrayIndexes[arrayIndexes.length - 1];
        deepestPath = parts.slice(0, lastIndex + 1).join('.');
      }
    }

    return deepestPath;
  }

  /* ---- FULL NESTED ARRAY JSON PATH RESOLVER ---- */
  extractByJsonPath(obj, path) {
  if (!obj || !path) {
    // console.log('[JSONPATH] Invalid input', { obj, path });
    return null;
  }

  const parts = path.split('.');

  const walk = (current, index) => {
    // console.log('[JSONPATH] Walk', {
      // index,
      // part: parts[index],
      // currentType: Array.isArray(current) ? 'array' : typeof current,
      // currentValue: current
    // });

    if (current === null || current === undefined) {
      // console.log('[JSONPATH] ❌ Current is null/undefined');
      return [];
    }

    if (index === parts.length) {
      // console.log('[JSONPATH] ✅ End reached:', current);
      return [current];
    }

    const part = parts[index];

    // Handle array wildcard
    if (part.endsWith('[*]')) {
      const key = part.replace('[*]', '');
      const arr = key ? current[key] : current;

      if (!Array.isArray(arr)) {
        // console.log('[JSONPATH] ❌ Expected array but got:', arr);
        return [];
      }

      // console.log('[JSONPATH] 🔁 Array found, length:', arr.length);
      return arr.flatMap((item, i) => {
        // console.log(`[JSONPATH] → Array item ${i}`, item);
        return walk(item, index + 1);
      });
    }

    // Normal object key
    if (!(part in current)) {
      // console.log('[JSONPATH] ❌ Key not found:', part);
      return [];
    }

    return walk(current[part], index + 1);
  };

  const result = walk(obj, 0);

  // console.log('[JSONPATH] 🔚 Final result:', result);

  if (!result.length) return null;
  return result.length === 1 ? result[0] : result;
}


  applyMapping(record, mapping) {
  let value;

  
  // 🔹 CASE: Combined date|time fields (DB or JSON flat record)
  if (
    mapping.pvfm_json_path &&
    mapping.pvfm_json_path.includes('|')
  ) {
    return this.buildTimestamp(record, mapping);
  }
  
  if (['api','json','xml','soap','multiapi'].includes(this.sourceType)) {
       // ITEM / PAYMENT TABLES
    if (
      mapping.pvfm_tablename !== 'raw_transactions' &&
      mapping.pvfm_row_root_json_path
    ) {

      // 1️⃣ Try direct field first
      value = record?.[mapping.pvfm_json_path];

      // 2️⃣ If undefined AND nested path exists → resolve nested
      if (
        value === undefined &&
        mapping.pvfm_json_path?.includes('.')
      ) {
        value = this.extractByJsonPath(record, mapping.pvfm_json_path);
      }

    // TRANSACTION HEADER
    } else {

      if (mapping.pvfm_tablename === 'raw_transactions' && !mapping.pvfm_row_root_json_path) {
        // console.log('Mapping transaction field without row root', mapping.pvfm_json_path)
    value = record?.[mapping.pvfm_json_path];
      } else {
      
      value = mapping.pvfm_json_path
        ? this.extractByJsonPath(record, mapping.pvfm_json_path)
        : undefined;
      }
    }
  } else {
    value = mapping.pvfm_source_field
      ? record[mapping.pvfm_source_field]
      : undefined;
  }

  if (mapping.pvfm_transform_rule && value != null) {
    value = this.applyTransformation(value, mapping.pvfm_transform_rule);
  }
// console.log('Applied mapping ', { mapping, value });
  return value;
}

/**
 * Build timestamp from separate date & time fields
 * Supports:
 *  - Date: YYYYMMDD, YYYY-MM-DD
 *  - Time: HHMMSS, HH:mm:ss, HH:mm:ss.micro
 *
 * Output:
 *  - YYYY-MM-DD HH:mm:ss
 */
buildTimestamp(record, mapping) {
  const [dateKey, timeKey] = mapping.pvfm_json_path.split('|');

  const dt = record?.[dateKey];
  const tm = record?.[timeKey];

  if (!dt || !tm) return null;

  let datePart;
  let timePart;

  // -----------------------------
  // DATE FORMAT HANDLING
  // -----------------------------
  if (/^\d{8}$/.test(dt)) {
    // YYYYMMDD
    datePart = `${dt.slice(0, 4)}-${dt.slice(4, 6)}-${dt.slice(6, 8)}`;
  } else if (/^\d{4}-\d{2}-\d{2}$/.test(dt)) {
    // YYYY-MM-DD
    datePart = dt;
  } else {
    this.logger.warn(`Invalid date format: ${dt}`);
    return null;
  }

  // -----------------------------
  // TIME FORMAT HANDLING
  // -----------------------------
  if (/^\d{6}$/.test(tm)) {
    // HHMMSS
    timePart = `${tm.slice(0, 2)}:${tm.slice(2, 4)}:${tm.slice(4, 6)}`;
  } else if (/^\d{2}:\d{2}:\d{2}/.test(tm)) {
    // HH:mm:ss or HH:mm:ss.micro
    timePart = tm.slice(0, 8);
  } else {
    this.logger.warn(`Invalid time format: ${tm}`);
    return null;
  }

  let value = `${datePart} ${timePart}`;

  // -----------------------------
  // TRANSFORMATION RULE
  // -----------------------------
  if (mapping.pvfm_transform_rule) {
    value = this.applyTransformation(value, mapping.pvfm_transform_rule);
  }

  return value;
}


  /*applyMapping(record, mapping, arrayRoot) {
    let value;
    if (['api','json','xml','soap'].includes(this.sourceType)) {
      if (mapping.pvfm_json_path) {
        const relativePath = arrayRoot ? mapping.pvfm_json_path.replace(`${arrayRoot}[*].`, '') : mapping.pvfm_json_path;
        value = this.extractByJsonPath(record, relativePath);
      }
    } else if (['db','database'].includes(this.sourceType)) {
      if (mapping.pvfm_source_field) value = record[mapping.pvfm_source_field];
    }

    if (value !== undefined && value !== null && mapping.pvfm_transform_rule) {
      value = this.applyTransformation(value, mapping.pvfm_transform_rule);
    }
console.log('Applied mapping 1 ', { mapping, value });
    return value;
    
  }*/

  applyTransformation(value, rule) {
    try {
      if (rule.includes('epochToDate')) {
      return dayjs.unix(Number(value)).tz('Asia/Kolkata').format('YYYY-MM-DD');
    }

    if (rule.includes('epochToTimestamp')) {
      return dayjs.unix(Number(value)).tz('Asia/Kolkata').toDate();
    }
      if (rule.includes('toUpperCase')) return String(value).toUpperCase();
      if (rule.includes('toLowerCase')) return String(value).toLowerCase();
      if (rule.includes('parseFloat')) return parseFloat(value);
      if (rule.includes('parseInt')) return parseInt(value);
      if (rule.includes('toISOString')) return new Date(value).toISOString();
      // 🔹 Handle parseDateTime dynamically
    if (rule.includes('parseDateTime')) return this.parseApiDateTime(value);

      return value;
    } catch (err) {
      this.logger.warn('Transformation failed', { rule, value, error: err.message });
      return value;
    }
  }

  splitTransactionDateTime(timestamp) {
  if (!timestamp) {
    return {
      transaction_date: null,
      transaction_time: null
    };
  }

  // Convert "2025-12-31 06:05:31+05:30" → ISO-safe
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


  async parseXml(xmlString) {
    return new Promise((resolve, reject) => {
      xml2js.parseString(xmlString, { explicitArray: false }, (err, result) => {
        if (err) return reject(err);
        resolve(result);
      });
    });
  }
}

module.exports = FieldMapper;

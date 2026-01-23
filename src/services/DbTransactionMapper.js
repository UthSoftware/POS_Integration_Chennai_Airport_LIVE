const { v4: uuidv4 } = require('uuid');

class DbTransactionMapper {
  constructor(config, fieldMappings) {
    this.config = config;
    this.fieldMappings = fieldMappings;
    this.sourceType = config.cac_apidbmapping?.toLowerCase() || 'db';
  }

  /* ========================= PUBLIC ========================== */
  mapTransactions(dbRows) {
    
    let rows = dbRows;

  if (!Array.isArray(rows)) {
    if (dbRows?.AllData && Array.isArray(dbRows.AllData)) {
      rows = dbRows.AllData;
    } else {
      console.error('❌ Invalid DB response shape', dbRows);
      return [];
    }
  }

  // console.log('Mapping DB transactions, total rows:', rows.length);
  if (!rows.length) return [];

  const invoiceMapping = this.getInvoiceMapping();
  if (!invoiceMapping) {
    throw new Error('Invoice number mapping not found');
  }

  // 🔹 Group rows using mapping (DB / JSON safe)
  const grouped = {};

  for (const row of rows) {
    const invoiceNo = this.applyMapping(row, invoiceMapping);

    if (!invoiceNo) continue;

    if (!grouped[invoiceNo]) {
      grouped[invoiceNo] = [];
    }

    grouped[invoiceNo].push(row);
  }

  const result = [];

  for (const invoiceNo of Object.keys(grouped)) {
    const rows = grouped[invoiceNo];
    const headerRow = rows[0];

      // 1️⃣ Build HEADER (canonical)
      const tx = this.mapTransactionHeader(headerRow);

      // 2️⃣ Build ITEMS (read ONLY from header)
      tx.items = rows.map(row => this.mapItem(row, tx));

      // 3️⃣ Build PAYMENTS (read ONLY from header)
      tx.payments = this.mapPayment(headerRow, tx);

      result.push(tx);
    }

    return result;
  }

  /* ========================= HEADER ========================== */
  mapTransactionHeader(row) {
    // Canonical header (NO DB COLUMN NAMES HERE)
    const tx = {
      transaction_id: uuidv4(),
      batch_id: uuidv4(),
      source_system: this.config.cac_pos_vendor,
      agent_id: this.config.cac_config_id,
      brand_id: this.config.com_brand_id,
      brand_name: this.config.brand_name,
      outlet_id: this.config.com_outlet_id,
      outlet_name: this.config.cac_outlet_id,
      terminal: this.config.com_terminal,
      gate: this.config.com_gate,
      transaction_type: 'SALE'
    };

    const headerMappings = this.getMappings('raw_transactions');

    for (const m of headerMappings) {
      const value = this.applyMapping(row, m);
      if (value !== undefined && value !== null) {
        tx[m.pvfm_source_field] = value; // canonical assignment
      }
    }
// console.log('Mapped transaction', {invoice_no: tx.invoice_no,tx});
    return tx;
  }

  /* ========================= ITEMS ========================== */
  mapItem(row, tx) {
    const item = {
      item_line_id: uuidv4(),
      transaction_id: tx.transaction_id,
      brand_id: tx.brand_id,
      brand_name: tx.brand_name,
      outlet_id: tx.outlet_id,
      outlet_name: tx.outlet_name,
      terminal: tx.terminal,
      gate: tx.gate,
      transaction_time: tx.transaction_time,
      invoice_no: tx.invoice_no
    };

    const itemMappings = this.getMappings('raw_transaction_items');
    for (const m of itemMappings) {
      const value = this.applyMapping(row, m);
      if (value !== undefined && value !== null) {
        item[m.pvfm_source_field] = value;
      }
    }

    return item;
  }

  /* ========================= PAYMENT ========================== */
  mapPayment(row, tx) {
    const payment = {
      payment_id: uuidv4(),
      transaction_id: tx.transaction_id,
      brand_id: tx.brand_id,
      brand_name: tx.brand_name,
      outlet_id: tx.outlet_id,
      outlet_name: tx.outlet_name,
      terminal: tx.terminal,
      gate: tx.gate,
      transaction_time: tx.transaction_time,
      invoice_no: tx.invoice_no
    };

    const payMappings = this.getMappings('raw_payment');
    for (const m of payMappings) {
      const value = this.applyMapping(row, m);
      if (value !== undefined && value !== null) {
        payment[m.pvfm_source_field] = value;
      }
    }

    return [payment];
  }

  /* ========================= HELPERS ========================== */
  getMappings(table) {
    return this.fieldMappings.filter(
      m => m.pvfm_tablename === table
    );
  }

  groupBy(rows, key) {
    return rows.reduce((acc, r) => {
      const k = r[key];
      if (!acc[k]) acc[k] = [];
      acc[k].push(r);
      return acc;
    }, {});
  }

  /* ========================= MAPPING CORE ========================== */
  applyMapping(record, mapping) {
    let value;

    // 🔹 DB DATE | TIME (vendor-independent)
    if (
      mapping.pvfm_target_field &&
      mapping.pvfm_target_field.includes('|')
    ) {
      const [dateCol, timeCol] =
        mapping.pvfm_target_field.split('|');

      return this.buildTimestamp(
        record[dateCol],
        record[timeCol]
      );
    }

    // 🔹 JSON / API
    if (['api', 'json', 'xml', 'soap'].includes(this.sourceType)) {
      if (mapping.pvfm_json_path) {
        value = this.extractByJsonPath(record, mapping.pvfm_json_path);
      }
    }
    // 🔹 DB
    else {
      if (mapping.pvfm_target_field) {
        value = record[mapping.pvfm_target_field];
      }
    }

    // 🔹 Transform
    if (mapping.pvfm_transform_rule && value != null) {
      value = this.applyTransformation(value, mapping.pvfm_transform_rule);
    }

    return value;
  }

  /* ========================= DATE ========================== */
  buildTimestamp(date, time) {
    if (!date || !time) return null;
    return new Date(`${date}T${time}`);
  }

  /* ========================= TRANSFORM ========================== */
  applyTransformation(value, rule) {
    try {
      if (rule.includes('toUpperCase')) return String(value).toUpperCase();
      if (rule.includes('toLowerCase')) return String(value).toLowerCase();
      if (rule.includes('parseFloat')) return parseFloat(value);
      if (rule.includes('parseInt')) return parseInt(value);
      if (rule.includes('toISOString')) return new Date(value).toISOString();
      return value;
    } catch {
      return value;
    }
  }

  /* ========================= JSON PATH ========================== */
  extractByJsonPath(obj, path) {
    return path.split('.').reduce((o, k) => (o ? o[k] : undefined), obj);
  }

  getInvoiceMapping() {
  return this.fieldMappings.find(
    m =>
      m.pvfm_tablename === 'raw_transactions' &&
      m.pvfm_source_field === 'invoice_no'
  );
}
}


module.exports = DbTransactionMapper;

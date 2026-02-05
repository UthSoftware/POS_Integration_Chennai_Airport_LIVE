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
    console.log('Mapped transaction', { invoice_no: tx.invoice_no, tx });
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

    if (typeof value === 'string') {
      if (/^\d{2}\/\d{2}\/\d{4} \d{2}:\d{2}:\d{2} (AM|PM)$/i.test(value)) {
        value = this.normalizeVendorDateTime(value);
      } else if (/^\d{2}\/\d{2}\/\d{4}$/.test(value)) {
        value = this.normalizeVendorDate(value);
      }
    }

    return value;
  }

  /* ========================= DATE ========================== */
  buildTimestamp(date, time) {
    // if (!date || !time) return null;
    // return new Date(`${date}T${time}`);
    if (!date || !time) return null;

    // Handle DD/MM/YYYY
    const [day, month, year] = date.split('/').map(Number);

    // Handle HH:MM:SS AM/PM
    const [timePart, meridian] = time.split(' ');
    let [hours, minutes, seconds] = timePart.split(':').map(Number);

    if (meridian === 'PM' && hours !== 12) hours += 12;
    if (meridian === 'AM' && hours === 12) hours = 0;

    // ✅ Return ISO-safe value
    return new Date(
      year,
      month - 1,
      day,
      hours,
      minutes,
      seconds
    ).toISOString();
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
    // return dt
    // .tz('Asia/Kolkata')
    // .format('YYYY-MM-DD HH:mm:ss');

    return dt(
      year,
      month - 1,
      day,
      hh,
      mm,
      ss
    ).toISOString();
  }

  /* ========================= TRANSFORM ========================== */
  applyTransformation(value, rule) {
    try {
      if (rule.includes('normalizeVendorDate'))
        return this.normalizeVendorDate(value);

      if (rule.includes('normalizeVendorDateTime'))
        return this.normalizeVendorDateTime(value);

      if (rule.includes('toUpperCase')) return String(value).toUpperCase();
      if (rule.includes('toLowerCase')) return String(value).toLowerCase();
      if (rule.includes('parseFloat')) return parseFloat(value);
      if (rule.includes('parseInt')) return parseInt(value);
      if (rule.includes('toISOString')) return new Date(value).toISOString();
      if (rule.includes('parseDateTime')) return this.parseApiDateTime(value);
      // 🛡️ Absolute safety: never send vendor dates to DB

      // console.log('value', value);
      return value;
    } catch {
      return value;
    }
  }

  normalizeVendorDateTime(value) {
    if (!value || typeof value !== 'string') return value;

    // Match: DD/MM/YYYY HH:MM:SS AM|PM
    const match = value.match(
      /^(\d{2})\/(\d{2})\/(\d{4}) (\d{2}):(\d{2}):(\d{2}) (AM|PM)$/i
    );

    if (!match) return value; // not a datetime

    let [, day, month, year, hh, mm, ss, meridian] = match;

    hh = parseInt(hh, 10);
    if (meridian.toUpperCase() === 'PM' && hh !== 12) hh += 12;
    if (meridian.toUpperCase() === 'AM' && hh === 12) hh = 0;

    return new Date(
      year,
      month - 1,
      day,
      hh,
      mm,
      ss
    ).toISOString();
    // return dayjs(value, 'DD/MM/YYYY hh:mm:ss A')
    // .format('YYYY-MM-DD HH:mm:ss');
  }

  normalizeVendorDate(value) {
    if (!value || typeof value !== 'string') return value;

    // Match DD/MM/YYYY
    const match = value.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
    if (!match) return value;

    const [, day, month, year] = match;
    return `${year}-${month}-${day}`; // ISO date
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

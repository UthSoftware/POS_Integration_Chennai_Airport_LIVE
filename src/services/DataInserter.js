const pool = require('../config/database');
const { v4: uuidv4 } = require('uuid');

const createLogger = require('../config/logger');

class DataInserter {
  constructor(config) {
    this.config = config;
    this.pool = pool;
    this.logger = createLogger(config.vendor_name || 'inserter');
  }

  async insertTransactions(transactions) {
    const client = await pool.connect();
    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;

    try {
      await client.query('BEGIN');

      for (const transaction of transactions) {
        try {
          // 🔹 Check if transaction already exists
          const exists = await this.checkTransactionExists(client, transaction);

          if (exists) {
            skippedCount++;
            this.logger.info('Transaction already exists, skipping', {
              transaction_id: transaction.transaction_id,
              invoice_no: transaction.invoice_no
            });
            continue;
          }

          await this.insertTransaction(client, transaction);
          successCount++;
        } catch (error) {
          errorCount++;
          this.logger.error('Transaction insert failed', {
            error: error.message,
            transaction_id: transaction.transaction_id
          });

          await this.logException(client, {
            transaction_id: transaction.transaction_id,
            brand_id: transaction.brand_id,
            brand_name: transaction.brand_name,
            outlet_id: transaction.outlet_id,
            outlet_name: transaction.outlet_name,
            event_type: 'INSERT_ERROR',
            terminal: transaction.terminal,
            gate: transaction.gate,
            user: 'system',
            reason: error.message,
            details: { transaction, error: error.stack }
          });
        }
      }

      await client.query('COMMIT');
      this.logger.info('Batch insert completed', {
        successCount,
        errorCount,
        skippedCount
      });

      return { successCount, errorCount, skippedCount };
    } catch (error) {
      await client.query('ROLLBACK');
      this.logger.error('Batch insert failed', { error: error.message });
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Check if a transaction already exists in the database
   */
  async checkTransactionExists(client, transaction) {

    const query = `
      SELECT 1 FROM raw_transactions
      WHERE invoice_no = $1 and brand_name=$2 AND outlet_name=$3
      LIMIT 1
    `;

    const result = await client.query(query, [transaction.invoice_no, transaction.brand_name, transaction.outlet_name]);
    // console.log('Check transaction exists', { invoice_no: transaction.invoice_no, exists: result.rows.length > 0 });
    return result.rows.length > 0;
  }

  /**
   * Check if transaction items already exist
   */
  async checkItemsExist(client, invoice_no, brand_name, outlet_name) {
    const query = `
      SELECT 1 FROM raw_transaction_items
      WHERE invoice_no = $1 and brand_name=$2 AND outlet_name=$3
      LIMIT 1
    `;

    const result = await client.query(query, [invoice_no, brand_name, outlet_name]);
    return result.rows.length > 0;
  }

  /**
   * Check if payments already exist
   */
  async checkPaymentsExist(client, invoice_no, brand_name, outlet_name) {
    const query = `
      SELECT 1 FROM raw_payment
      WHERE invoice_no = $1 and brand_name=$2 AND outlet_name=$3
      LIMIT 1
    `;

    const result = await client.query(query, [invoice_no, brand_name, outlet_name]);
    return result.rows.length > 0;
  }

  async insertTransaction(client, transaction) {
    const query = `
      INSERT INTO raw_transactions (
        transaction_id, source_transaction_ref, source_system, agent_id, batch_id,
        brand_id, brand_name, outlet_id, outlet_name, terminal, gate,
        transaction_time, transaction_date, shift, shiftdate, salesret_amt,
        transaction_type, gross_amount, discount_amount, tax_amount, service_charge,
        fees_amount, net_amount, currency, exchange_rate_to_mc, payment_summary,
        tender_types, item_count, avg_item_price, customer_type, flight_info,
        tax_breakdown, promo_ids, void_reason, closed_by, device_id, meta,invoice_no
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
           ($13::timestamptz AT TIME ZONE 'Asia/Kolkata')::date, $14, $15, $16,
        $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
        $31, $32, $33, $34, $35, $36,$37, $38
      )
    `;

    const values = [
      transaction.transaction_id,
      transaction.source_transaction_ref,
      transaction.source_system,
      transaction.agent_id,
      transaction.batch_id,
      transaction.brand_id,
      transaction.brand_name,
      transaction.outlet_id,
      transaction.outlet_name,
      transaction.terminal,
      transaction.gate,
      transaction.transaction_time,
      // transaction.transaction_date,
      transaction.received_at,
      transaction.shift,
      transaction.shiftdate,
      transaction.salesret_amt,
      transaction.transaction_type,
      transaction.gross_amount,
      transaction.discount_amount,
      transaction.tax_amount,
      transaction.service_charge,
      transaction.fees_amount,
      transaction.net_amount,
      transaction.currency,
      transaction.exchange_rate_to_mc,
      JSON.stringify(transaction.payment_summary),
      transaction.tender_types,
      transaction.item_count,
      transaction.avg_item_price,
      transaction.customer_type,
      JSON.stringify(transaction.flight_info),
      JSON.stringify(transaction.tax_breakdown),
      transaction.promo_ids,
      transaction.void_reason,
      transaction.closed_by,
      transaction.device_id,
      JSON.stringify(transaction.meta),
      transaction.invoice_no
    ];

    // console.log('Inserting transaction', { values: values });

    await client.query(query, values);
  }

  async insertTransactionItems(client, items, transaction) {
    if (!items || items.length === 0) return;

    // 🔹 Check if items already exist
    const itemsExist = await this.checkItemsExist(client, transaction.invoice_no, transaction.brand_name, transaction.outlet_name);

    if (itemsExist) {
      this.logger.info('Transaction items already exist, skipping', {
        transaction_id: transaction.transaction_id
      });
      return;
    }

    // 🔹 Aggregate arrays
    const aggregated = {
      item_line_id: uuidv4(), // one ID per transaction
      transaction_id: transaction.transaction_id,
      invoice_no: transaction.invoice_no,
      brand_id: transaction.brand_id,
      brand_name: transaction.brand_name,
      outlet_id: transaction.outlet_id,
      outlet_name: transaction.outlet_name,
      terminal: transaction.terminal,
      gate: transaction.gate,
      transaction_time: transaction.transaction_time,
      received_at: transaction.received_at,

      sku: items.map(i => i.sku),
      item_name: items.map(i => i.sku_title || i.item_name),
      category: items.map(i => i.sku_category),
      subcategory: items.map(i => i.subcategory || null),
      // quantity: items.map(() => 1),
      // unit_price: items.map(() => null),
      // line_total: items.map(() => null),
      // line_discount: items.map(() => null),
      // line_tax: items.map(() => null),
      quantity: items.map(i => Number(i.quantity ?? 1)),

      unit_price: items.map(i =>
        i.unit_price === undefined || i.unit_price === null
          ? null
          : Number(i.unit_price)
      ),

      line_total: items.map(i =>
        i.line_total === undefined || i.line_total === null
          ? null
          : Number(i.line_total)
      ),

      line_discount: items.map(i =>
        i.line_discount === undefined || i.line_discount === null
          ? null
          : Number(i.line_discount)
      ),

      line_tax: items.map(i =>
        i.line_tax === undefined || i.line_tax === null
          ? null
          : Number(i.line_tax)
      ),

      hsncode: items.map(i => i.hsncode || null),
      cess: items.map(i => i.cess || 0),
      taxpercentage: items.map(i => i.taxpercentage || 0),
      cgst: items.map(i => i.cgst || 0),
      sgst: items.map(i => i.sgst || 0),
      // tax_details: items.map(i => i.tax_details || null),

      // modifiers: {},
      // void_flag: false,
      // meta: items,

      shiftdate: transaction.shiftdate,
      shift: transaction.shift,
      transtype: transaction.transaction_type
    };

    const query = `
      INSERT INTO raw_transaction_items (
        item_line_id, transaction_id, invoice_no,
        brand_id, brand_name, outlet_id, outlet_name,
        terminal, gate, transaction_time, transaction_date,
        sku, item_name, category, subcategory,
        quantity, unit_price, line_total, line_discount, line_tax,
        taxpercentage, cgst, sgst, shiftdate, shift, transtype,cess,hsncode
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
           ($11::timestamptz AT TIME ZONE 'Asia/Kolkata')::date,
        $12,$13,$14,$15,$16,$17,$18,$19,$20,
        $21,$22,$23,$24,$25,$26,$27,$28
      )
    `;

    const values = [
      aggregated.item_line_id,
      aggregated.transaction_id,
      aggregated.invoice_no,
      aggregated.brand_id,
      aggregated.brand_name,
      aggregated.outlet_id,
      aggregated.outlet_name,
      aggregated.terminal,
      aggregated.gate,
      aggregated.transaction_time,
      // aggregated.transaction_date,
      aggregated.received_at,

      aggregated.sku,
      aggregated.item_name,
      aggregated.category,
      aggregated.subcategory,
      aggregated.quantity,
      aggregated.unit_price,
      aggregated.line_total,
      aggregated.line_discount,
      aggregated.line_tax,

      aggregated.taxpercentage,
      aggregated.cgst,
      aggregated.sgst,
      aggregated.shiftdate,
      aggregated.shift,
      aggregated.transtype,
      aggregated.cess,
      aggregated.hsncode
    ];

    // this.logger.info('Inserting aggregated items', {

    // values: values
    // });


    await client.query(query, values);
  }

  async insertPayments(client, payments, transaction) {
    if (!payments || payments.length === 0) return;

    // 🔹 Check if payments already exist
    const paymentsExist = await this.checkPaymentsExist(client, transaction.invoice_no, transaction.brand_name, transaction.outlet_name);

    if (paymentsExist) {
      this.logger.info('Payments already exist, skipping', {
        transaction_id: payments[0].transaction_id
      });
      return;
    }

    for (const payment of payments) {
      const query = `
        INSERT INTO raw_payment (
          payment_id, transaction_id, source_payment_ref, brand_id, brand_name,
          outlet_id, outlet_name, terminal, gate, transaction_time, transaction_date,
          payment_type, amount, card_scheme, issuer_bank, invoice_no, currency,
          exchange_rate_to_mc, meta, shift, shiftdate, transtype
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
           ($11::timestamptz AT TIME ZONE 'Asia/Kolkata')::date, $12, $13, $14, $15,
          $16, $17, $18, $19, $20, $21,$22
        )
      `;

      const values = [
        payment.payment_id,
        payment.transaction_id,
        payment.invoice_no,
        payment.brand_id,
        payment.brand_name,
        payment.outlet_id,
        payment.outlet_name,
        payment.terminal,
        payment.gate,
        payment.transaction_time,
        // payment.transaction_date,
        payment.received_at,
        payment.payment_type,
        payment.amount,
        payment.card_scheme,
        payment.issuer_bank,
        payment.invoice_no,
        payment.currency,
        payment.exchange_rate_to_mc,
        JSON.stringify(payment.meta),
        payment.shift,
        payment.shiftdate,
        payment.transtype
      ];
      // console.log('Inserting payment', { payment_id: values });
      await client.query(query, values);
    }
  }

  async logException(client, exception) {
    const query = `
      INSERT INTO raw_exceptions (
        transaction_id, brand_id, brand_name, outlet_id, outlet_name,
        event_type, terminal, gate, "user", reason, amount, details
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    `;

    const values = [
      exception.transaction_id,
      exception.brand_id,
      exception.brand_name,
      exception.outlet_id,
      exception.outlet_name,
      exception.event_type,
      exception.terminal,
      exception.gate,
      exception.user,
      exception.reason,
      exception.amount,
      JSON.stringify(exception.details)
    ];

    await client.query(query, values);
  }

  async logIngestion(logData) {
    const query = `
      INSERT INTO ingestion_log (
        agent_id, batch_id, source_system, outlet_id, outlet_name,
        brand_id, brand_name, terminal, gate, records_count, errors_count,
        first_received_at, last_received_at, status, meta
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
    `;

    const values = [
      logData.agent_id,
      logData.batch_id,
      logData.source_system,
      logData.outlet_id,
      logData.outlet_name,
      logData.brand_id,
      logData.brand_name,
      logData.terminal,
      logData.gate,
      logData.records_count,
      logData.errors_count,
      logData.first_received_at,
      logData.last_received_at,
      logData.status,
      JSON.stringify(logData.meta)
    ];

    await pool.query(query, values);
  }
}

module.exports = DataInserter;
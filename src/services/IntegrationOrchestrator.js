const { v4: uuidv4 } = require('uuid');
const ConfigModel = require('../models/ConfigModel');
const DataFetcher = require('./DataFetcher');
const FieldMapper = require('./FieldMapper');
const DataInserter = require('./DataInserter');
const createLogger = require('../config/logger');
const fs = require('fs').promises;
const path = require('path');

class IntegrationOrchestrator {
  constructor() {
    this.logger = createLogger('orchestrator');
  }

  async executeIngestion() {
    this.logger.info('Starting data ingestion cycle');

    try {
      // Load vendor filter from vendordetails.txt
      const vendorFilter = await this.loadVendorFilter();
      
      const configs = await ConfigModel.getActiveConfigs(vendorFilter);
      this.logger.info(`Found ${configs.length} active configurations`);

      for (const config of configs) {
        await this.processConfig(config);
      }

      this.logger.info('Data ingestion cycle completed');
    } catch (error) {
      this.logger.error('Ingestion cycle failed', { error: error.message, stack: error.stack });
    }
  }

  async loadVendorFilter() {
    try {
      const vendorFilePath = path.join(__dirname, '../../vendordetails.txt');
      const content = await fs.readFile(vendorFilePath, 'utf-8');
      
      const lines = content.split('\n');
      const vendorLine = lines.find(line => line.startsWith('001|vendor:'));
      
      if (vendorLine) {
        const vendorName = vendorLine.split('vendor:')[1]?.trim();
        if (vendorName) {
          this.logger.info('Vendor filter loaded', { vendor: vendorName });
          return [vendorName];
        }
      }
      
      this.logger.warn('No vendor filter found in vendordetails.txt');
      return null;
    } catch (error) {
      this.logger.warn('Could not load vendor filter', { error: error.message });
      return null;
    }
  }

  async processConfig(config) {
    const batchId = uuidv4();
    const startTime = new Date();
    
    this.logger.info('Processing configuration', {
      configId: config.cac_config_id,
      vendor: config.vendor_name,
      outlet: config.cac_outlet_id,
      sourceType: config.cac_jsonordb,
      CUSTOMERID: config.cac_customer_id,
      URL: config.cac_api_url
    });

    const inserter = new DataInserter(config);
    let client = null;

    try {
      // Step 1: Fetch data (with max date logic inside DataFetcher)
      const fetcher = new DataFetcher(config);
      const rawData = await fetcher.fetchData();

      this.logger.info('RAW API RESPONSE STRUCTURE123', {
  type: typeof rawData,
  isArray: Array.isArray(rawData),
  topKeys: rawData && typeof rawData === 'object'
    ? Object.keys(rawData)
    : 'NOT_OBJECT'
});


      if (!rawData || (Array.isArray(rawData) && rawData.length === 0)) {
        this.logger.warn('No data fetched', { configId: config.cac_config_id });
        return;
      }

      this.logger.info('Data fetched successfully', { 
        recordCount: Array.isArray(rawData) ? rawData.length : 1 
      });

      // Step 2: Get ALL field mappings for this vendor
      const allMappings = await ConfigModel.getAllFieldMappings(config.vendor_id);

      // console.log('Total field mappings retrieved:', config.vendor_id);

      // Step 3: Map data
      const mapper = new FieldMapper(config, [
        ...allMappings.raw_transactions,
        ...allMappings.raw_transaction_items,
        ...allMappings.raw_payment
      ]);
      
      const transactions = await mapper.mapTransactions(rawData);

      this.logger.info('Data mapped successfully', { 
        transactionCount: transactions.length 
      });

      // Step 4: Insert data in transaction with duplicate checking
      client = await inserter.pool.connect();
      
      let totalRecords = 0;
      let totalErrors = 0;
      let totalSkipped = 0;

      await client.query('BEGIN');

      try {
        for (const transaction of transactions) {
          try {
            // 🔹 Check if transaction already exists
            const exists = await inserter.checkTransactionExists(client, transaction);
            
            if (exists) {
              totalSkipped++;
              this.logger.info('Transaction already exists, skipping', {
                transaction_id: transaction.transaction_id,
                invoice_no: transaction.invoice_no
              });
              continue;
            }

            // Insert transaction
            await inserter.insertTransaction(client, transaction);
            
            // Insert items if available
            if (transaction.items && transaction.items.length > 0) {
              await inserter.insertTransactionItems(client, transaction.items, transaction);
            }
            // console.log('Inserted transaction', {payments: transaction.payments.length});
            // Insert payments if available
            if (transaction.payments && transaction.payments.length > 0) {
              await inserter.insertPayments(client, transaction.payments);
            }

            totalRecords++;
          } catch (error) {
            totalErrors++;
            this.logger.error('Transaction insert failed', {
              transactionId: transaction.transaction_id,
              error: error.message,
              stack: error.stack
            });

            // Log exception
            await inserter.logException(client, {
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

        // Step 5: Log successful ingestion
        await inserter.logIngestion({
          agent_id: config.cac_config_id,
          batch_id: batchId,
          source_system: config.cac_pos_vendor,
          outlet_id: config.com_outlet_id,
          outlet_name: config.cac_outlet_id,
          brand_id: config.com_brand_id,
          brand_name: config.brand_name,
          terminal: config.com_terminal,
          gate: config.com_gate,
          records_count: totalRecords,
          errors_count: totalErrors,
          first_received_at: startTime,
          last_received_at: new Date(),
          status: 'SUCCESS',
          meta: { 
            batchId, 
            configId: config.cac_config_id,
            skippedCount: totalSkipped
          }
        });

        this.logger.info('Configuration processed successfully', {
          configId: config.cac_config_id,
          records: totalRecords,
          errors: totalErrors,
          skipped: totalSkipped
        });

      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      }

    } catch (error) {
      this.logger.error('Configuration processing failed', {
        configId: config.cac_config_id,
        error: error.message,
        stack: error.stack
      });

      // Log failure
      await inserter.logIngestion({
        agent_id: config.cac_config_id,
        batch_id: batchId,
        source_system: config.cac_pos_vendor,
        outlet_id: config.com_outlet_id,
        outlet_name: config.cac_outlet_id,
        brand_id: config.com_brand_id,
        brand_name: config.brand_name,
        terminal: config.com_terminal,
        gate: config.com_gate,
        records_count: 0,
        errors_count: 1,
        first_received_at: startTime,
        last_received_at: new Date(),
        status: 'FAILED',
        meta: { error: error.message, stack: error.stack }
      });
    } finally {
      if (client) {
        client.release();
      }
    }
  }
}

module.exports = IntegrationOrchestrator;
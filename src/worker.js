require('dotenv').config();
const vendorQueue = require('./queue/vendorQueue');
const DbTransactionMapper = require('./services/DbTransactionMapper');
const DataInserter = require('./services/DataInserter');
const ConfigModel = require('./models/configModel');
const logger = require('./config/logger');

const CONCURRENCY = parseInt(process.env.QUEUE_CONCURRENCY) || 5;

async function processVendorData(job) {
  const { data, metadata } = job.data;
  
  try {
    logger.info('Processing vendor data job', {
      jobId: job.id,
      recordCount: Array.isArray(data) ? data.length : 1,
      metadata
    });

    job.progress(10);

    const config = await ConfigModel.getConfigById(metadata.configId);
    if (!config) {
      throw new Error(`Configuration not found: ${metadata.configId}`);
    }

    job.progress(20);

    const fieldMappings = await ConfigModel.getAllFieldMappings(config.cac_customer_id);
    
    job.progress(40);

    const mapper = new DbTransactionMapper(config, [
      ...fieldMappings.raw_transactions,
      ...fieldMappings.raw_transaction_items,
      ...fieldMappings.raw_payment
    ]);

    const transactions = mapper.mapTransactions(data);
    
    logger.info('Data mapped successfully', {
      jobId: job.id,
      transactionCount: transactions.length
    });

    job.progress(60);

    const inserter = new DataInserter(config);
    const result = await inserter.insertTransactions(transactions);

    job.progress(100);

    logger.info('Data inserted successfully', {
      jobId: job.id,
      success: result.successCount,
      errors: result.errorCount,
      skipped: result.skippedCount
    });

    return {
      success: true,
      recordsProcessed: transactions.length,
      inserted: result.successCount,
      errors: result.errorCount,
      skipped: result.skippedCount
    };

  } catch (error) {
    logger.error('Job processing error', {
      jobId: job.id,
      error: error.message,
      stack: error.stack
    });
    throw error;
  }
}

vendorQueue.queue.process(CONCURRENCY, processVendorData);

logger.info('Worker started', { concurrency: CONCURRENCY });

process.on('SIGINT', async () => {
  logger.info('Received SIGINT, shutting down worker');
  await vendorQueue.close();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  logger.info('Received SIGTERM, shutting down worker');
  await vendorQueue.close();
  process.exit(0);
});
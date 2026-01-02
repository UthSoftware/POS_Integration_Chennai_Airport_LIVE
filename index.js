require('dotenv').config();
const http = require('http');

const PORT = process.env.PORT || 3001;

// 🔹 Start HTTP server (MANDATORY for Webuzo)
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ status: 'ok' }));
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('POS Data Collector Agent is running\n');
});

server.listen(PORT, () => {
  console.log(`🌐 Server started and listening on port ${PORT}`);
});

const cron = require('node-cron');
const IntegrationOrchestrator = require('./src/services/IntegrationOrchestrator');

const ConfigValidator = require('./src/services/Configvalidator');
const VendorDataSeeder = require('./src/services/VendorDataSeeder');
const createLogger = require('./src/config/logger');
require('dotenv').config();


const logger = createLogger('main');
const orchestrator = new IntegrationOrchestrator();
const validator = new ConfigValidator();
const seeder = new VendorDataSeeder();

// Default sync interval from environment or 5 minutes
const syncInterval = process.env.DEFAULT_SYNC_INTERVAL || 5;

// Convert minutes to cron expression
const cronExpression = `*/${syncInterval} * * * *`;

let cronJob = null;
let isValidated = false;

// Startup function
async function startup() {
  console.log('\n╔════════════════════════════════════════════════════════════╗');
  console.log('║     POS Data Collector Agent - Starting Up                ║');
  console.log('╚════════════════════════════════════════════════════════════╝\n');

  logger.info('POS Data Collector Agent starting', { 
    syncInterval: `${syncInterval} minutes`,
    cronExpression 
  });

  // Step 0: Seed vendor data from file
  console.log('🌱 Step 0: Seeding vendor data from file...\n');
  try {
    await seeder.seedVendorData();
  } catch (error) {
    logger.error('Vendor data seeding failed', { error: error.message });
    console.error('⚠️  Vendor data seeding failed, continuing with validation...\n');
  }

  // Step 1: Validate required configuration tables
  console.log('🔍 Step 1: Validating configuration tables...\n');
  const validationResult = await validator.validateRequiredTables();

  if (!validationResult.isValid) {
    logger.error('Configuration validation failed - Cron job will NOT start', {
      missingTables: validationResult.missingTables
    });
    
    console.log('❌ CRON JOB WILL NOT START');
    console.log('   Reason: Required configuration tables are empty\n');
    console.log('💡 Options:');
    console.log('   1. Add vendor details to /workspace/pos-integrator/vendor_details/vendordetails.txt');
    console.log('   2. Use the REST API to add configuration data');
    console.log('   3. Manually insert data into the database\n');
    console.log('   Then restart the application.\n');
    
    isValidated = false;
    return false;
  }

  // Step 2: Check for active configurations
  console.log('🔍 Step 2: Checking active configurations...\n');
  await validator.checkActiveConfigurations();

  // Step 3: Start cron job
  console.log('🚀 Step 3: Starting cron scheduler...\n');
  startCronJob();
  
  isValidated = true;
  return true;
}

// Start cron job
function startCronJob() {
  cronJob = cron.schedule(cronExpression, async () => {
    logger.info('Scheduled ingestion triggered');
    console.log(`\n⏰ [${new Date().toISOString()}] Scheduled ingestion triggered`);
    
    try {
      await orchestrator.executeIngestion();
      console.log(`✅ [${new Date().toISOString()}] Ingestion completed successfully\n`);
    } catch (error) {
      logger.error('Scheduled ingestion failed', { error: error.message });
      console.error(`❌ [${new Date().toISOString()}] Ingestion failed: ${error.message}\n`);
    }
  });

  console.log('✅ Cron scheduler started successfully');
  console.log(`   Schedule: Every ${syncInterval} minutes (${cronExpression})`);
  console.log(`   Next run: ${getNextRunTime()}\n`);
  
  logger.info('Cron scheduler started', { cronExpression, syncInterval });
}

// Get next run time
function getNextRunTime() {
  const now = new Date();
  const nextRun = new Date(now.getTime() + syncInterval * 60000);
  return nextRun.toLocaleString();
}

// Run initial ingestion
async function runInitialIngestion() {
  if (!isValidated) {
    logger.warn('Skipping initial ingestion - validation failed');
    return;
  }

  console.log('🔄 Step 4: Running initial data ingestion...\n');
  logger.info('Running initial ingestion');
  
  try {
    await orchestrator.executeIngestion();
    console.log('✅ Initial ingestion completed successfully\n');
    logger.info('Initial ingestion completed successfully');
  } catch (error) {
    logger.error('Initial ingestion failed', { error: error.message });
    console.error(`❌ Initial ingestion failed: ${error.message}\n`);
  }
}

// Graceful shutdown
function gracefulShutdown(signal) {
  console.log(`\n\n🛑 Received ${signal}, shutting down gracefully...`);
  logger.info(`Received ${signal}, shutting down gracefully`);
  
  if (cronJob) {
    cronJob.stop();
    console.log('✅ Cron job stopped');
    logger.info('Cron job stopped');
  }
  
  console.log('👋 POS Data Collector Agent stopped\n');
  process.exit(0);
}

// Signal handlers
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

// Unhandled rejection handler
process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled Rejection', { reason, promise });
  console.error('❌ Unhandled Rejection:', reason);
});
// require('http').createServer(() => {}).listen(process.env.PORT || 3000);

// Start the application
(async () => {
  try {
    const validationPassed = await startup();
    
    if (validationPassed) {
      await runInitialIngestion();
      
      console.log('╔════════════════════════════════════════════════════════════╗');
      console.log('║   POS Data Collector Agent is running successfully        ║');
      console.log('╚════════════════════════════════════════════════════════════╝\n');
      console.log(`📊 Monitoring: Check logs in ./logs/ directory`);
      console.log(`🔍 Database: Check ingestion_log and raw_exceptions tables`);
      console.log(`⏱️  Next ingestion: ${getNextRunTime()}`);
      console.log(`\nPress Ctrl+C to stop\n`);
      
      logger.info('POS Data Collector Agent is running');
    } else {
      console.log('╔════════════════════════════════════════════════════════════╗');
      console.log('║   POS Data Collector Agent - Waiting for Configuration   ║');
      console.log('╚════════════════════════════════════════════════════════════╝\n');
      console.log('⏸️  Data collection is paused until configuration is added.');
      console.log('   The application will keep running for API access.');
      console.log(`\nPress Ctrl+C to stop\n`);
      
      logger.info('Application running in configuration mode - cron disabled');
    }
  } catch (error) {
    logger.error('Application startup failed', { error: error.message, stack: error.stack });
    console.error('\n❌ Application startup failed:', error.message);
    console.error('Please check the logs and database connection.\n');
    process.exit(1);
  }
})();
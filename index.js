require('dotenv').config();
const http = require('http');
const cron = require('node-cron');

const IntegrationOrchestrator = require('./src/services/IntegrationOrchestrator');
const ConfigValidator = require('./src/services/Configvalidator');
const VendorDataSeeder = require('./src/services/VendorDataSeeder');
const createLogger = require('./src/config/logger');

const PORT = process.env.PORT || 30035;

const logger = createLogger('main');
const orchestrator = new IntegrationOrchestrator();
const validator = new ConfigValidator();
const seeder = new VendorDataSeeder();

let cronJob = null;
let isValidated = false;

// ================= HTTP SERVER (REQUIRED FOR WEBUZO) =================
const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ status: 'ok', time: new Date() }));
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('POS Data Collector Agent is running\n');
});

server.listen(PORT, () => {
  console.log(`🌐 Server running on port ${PORT}`);
  logger.info(`HTTP server started on port ${PORT}`);
});
// ===================================================================

// ================= CRON CONFIG =================
const syncInterval = process.env.DEFAULT_SYNC_INTERVAL || 5;
const cronExpression = `*/${syncInterval} * * * *`;

// Startup
async function startup() {
  console.log('🌱 Seeding vendor data...');
  try {
    await seeder.seedVendorData();
  } catch (err) {
    logger.warn('Vendor seed failed', err.message);
  }

  console.log('🔍 Validating configuration...');
  const validation = await validator.validateRequiredTables();
  if (!validation.isValid) {
    console.log('❌ Validation failed. Cron disabled.');
    return;
  }

  await validator.checkActiveConfigurations();

  cronJob = cron.schedule(cronExpression, async () => {
    console.log(`⏰ Cron triggered at ${new Date().toISOString()}`);
    try {
      await orchestrator.executeIngestion();
      console.log('✅ Ingestion successful');
    } catch (err) {
      console.error('❌ Ingestion failed:', err.message);
      logger.error('Ingestion failed', err.message);
    }
  });

  isValidated = true;
  console.log(`✅ Cron scheduled every ${syncInterval} minutes`);
}

startup();

// ================= SHUTDOWN =================
function gracefulShutdown(signal) {
  console.log(`🛑 ${signal} received. Shutting down...`);
  if (cronJob) cronJob.stop();
  server.close(() => process.exit(0));
}

process.on('SIGINT', gracefulShutdown);
process.on('SIGTERM', gracefulShutdown);

process.on('unhandledRejection', err => {
  console.error('Unhandled Rejection:', err);
  logger.error('Unhandled Rejection', err);
});
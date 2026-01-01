require('dotenv').config();

const createLogger = require('./src/config/logger');
const { startServer } = require('./src/api/server'); // ✅ correct path

const logger = createLogger('root');

(async () => {
  try {
    logger.info('Starting POS Integration (API + Cron)');

    // 1️⃣ Start API
    startServer();
    logger.info('API server started');

    // 2️⃣ Start CRON
    // IMPORTANT: cron file is src/index.js
    require('./src/index'); // ✅ FIXED PATH

    logger.info('Cron agent started');
  } catch (err) {
    logger.error('Startup failed', {
      error: err.message,
      stack: err.stack
    });
    process.exit(1);
  }
})();

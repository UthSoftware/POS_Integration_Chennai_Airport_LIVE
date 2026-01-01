const winston = require('winston');
const path = require('path');
const fs = require('fs');

const createLogger = (vendorName = 'general') => {
  const logDir = path.join(__dirname, '../../logs', vendorName);
  
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }

  const today = new Date().toISOString().split('T')[0];
  const logFile = path.join(logDir, `${today}.log`);

  return winston.createLogger({
    level: process.env.LOG_LEVEL || 'info',
    format: winston.format.combine(
      winston.format.timestamp(),
      winston.format.errors({ stack: true }),
      winston.format.json()
    ),
    transports: [
      new winston.transports.File({ filename: logFile }),
      new winston.transports.Console({
        format: winston.format.combine(
          winston.format.colorize(),
          winston.format.simple()
        )
      })
    ]
  });
};

module.exports = createLogger;
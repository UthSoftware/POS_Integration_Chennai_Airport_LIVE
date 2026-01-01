const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const createLogger = require('../config/logger');
require('dotenv').config();

// Import routes
const vendorRoutes = require('./routes/vendorRoutes');
const configRoutes = require('./routes/configRoutes');
const outletRoutes = require('./routes/outletRoutes');
const fieldMappingRoutes = require('./routes/fieldMappingRoutes');

const app = express();
const logger = createLogger('api-server');
const PORT = process.env.API_PORT || 3000;

// Middleware
app.use(helmet()); // Security headers
app.use(cors()); // Enable CORS
app.use(express.json({ limit: '10mb' })); // Parse JSON bodies
app.use(express.urlencoded({ extended: true })); // Parse URL-encoded bodies

// Request logging middleware
app.use((req, res, next) => {
  logger.info('API Request', {
    method: req.method,
    path: req.path,
    ip: req.ip,
    userAgent: req.get('user-agent')
  });
  next();
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({
    success: true,
    status: 'healthy',
    timestamp: new Date().toISOString(),
    uptime: process.uptime()
  });
});

// API Routes
app.use('/api', vendorRoutes);
app.use('/api', configRoutes);
app.use('/api', outletRoutes);
app.use('/api', fieldMappingRoutes);

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    success: true,
    message: 'POS Integrator API',
    version: '1.0.0',
    endpoints: {
      vendors: {
        create: 'POST /api/vendors',
        list: 'GET /api/vendors',
        get: 'GET /api/vendors/:vendor_id',
        delete: 'DELETE /api/vendors/:vendor_id'
      },
      configs: {
        create: 'POST /api/configs',
        list: 'GET /api/configs',
        get: 'GET /api/configs/:config_id',
        delete: 'DELETE /api/configs/:config_id'
      },
      outlets: {
        create: 'POST /api/outlets',
        list: 'GET /api/outlets',
        get: 'GET /api/outlets/:outlet_id',
        delete: 'DELETE /api/outlets/:outlet_id'
      },
      fieldMappings: {
        create: 'POST /api/field-mappings',
        list: 'GET /api/field-mappings',
        get: 'GET /api/field-mappings/:mapping_id',
        delete: 'DELETE /api/field-mappings/:mapping_id'
      }
    }
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    success: false,
    message: 'Endpoint not found',
    path: req.path
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  logger.error('API Error', {
    error: err.message,
    stack: err.stack,
    path: req.path,
    method: req.method
  });

  res.status(err.status || 500).json({
    success: false,
    message: err.message || 'Internal server error',
    ...(process.env.NODE_ENV === 'development' && { stack: err.stack })
  });
});

// Start server
const startServer = () => {
  app.listen(PORT, () => {
    logger.info(`API Server started on port ${PORT}`);
    console.log(`\n🚀 POS Integrator API Server running on http://localhost:${PORT}`);
    console.log(`📚 API Documentation: http://localhost:${PORT}/`);
    console.log(`💚 Health Check: http://localhost:${PORT}/health\n`);
  });
};

// Graceful shutdown
process.on('SIGINT', () => {
  logger.info('Received SIGINT, shutting down API server gracefully');
  process.exit(0);
});

process.on('SIGTERM', () => {
  logger.info('Received SIGTERM, shutting down API server gracefully');
  process.exit(0);
});

module.exports = { app, startServer };
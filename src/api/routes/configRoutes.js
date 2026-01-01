const express = require('express');
const { body, param, validationResult } = require('express-validator');
const pool = require('../../config/database');
const createLogger = require('../../config/logger');
const { v4: uuidv4 } = require('uuid');
const { encrypt } = require('../../utils/encryption');

const router = express.Router();
const logger = createLogger('api-config');

// Validation middleware
const validate = (req, res, next) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    logger.warn('Validation failed', { errors: errors.array() });
    return res.status(400).json({ success: false, errors: errors.array() });
  }
  next();
};

// ============================================
// CUSTOMER API CONFIGS ROUTES
// ============================================

// CREATE - Add new API configuration
router.post('/configs',
  [
    body('cac_customer_id').notEmpty().withMessage('Customer ID is required'),
    body('cac_outlet_id').notEmpty().withMessage('Outlet ID is required'),
    body('cac_pos_vendor').notEmpty().withMessage('POS vendor is required'),
    body('cac_jsonordb').isIn(['json', 'api', 'db', 'database']).withMessage('Invalid source type'),
    body('cac_sync_interval_minutes').optional().isInt({ min: 1 }).withMessage('Sync interval must be positive integer'),
    body('cac_is_active').optional().isBoolean()
  ],
  validate,
  async (req, res) => {
    const {
      cac_customer_id, cac_outlet_id, cac_pos_vendor, cac_api_url, cac_http_method,
      cac_auth_type, cac_auth_header_key, cac_auth_header_value, cac_db_host,
      cac_db_port, cac_db_name, cac_db_username, cac_db_password, cac_sample_json,
      cac_field_mapping, cac_sync_interval_minutes, cac_is_active, cac_jsonordb
    } = req.body;

    const cac_config_id = uuidv4();

    try {
      // Encrypt sensitive fields
      const encrypted_auth_value = cac_auth_header_value ? encrypt(cac_auth_header_value) : null;
      const encrypted_db_password = cac_db_password ? encrypt(cac_db_password) : null;

      const query = `
        INSERT INTO customer_api_configs (
          cac_config_id, cac_customer_id, cac_outlet_id, cac_pos_vendor, cac_api_url,
          cac_http_method, cac_auth_type, cac_auth_header_key, cac_auth_header_value,
          cac_db_host, cac_db_port, cac_db_name, cac_db_username, cac_db_password,
          cac_sample_json, cac_field_mapping, cac_sync_interval_minutes, cac_is_active, cac_jsonordb
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
        RETURNING cac_config_id, cac_customer_id, cac_outlet_id, cac_pos_vendor, cac_is_active, cac_created_at
      `;

      const values = [
        cac_config_id, cac_customer_id, cac_outlet_id, cac_pos_vendor, cac_api_url,
        cac_http_method, cac_auth_type, cac_auth_header_key, encrypted_auth_value,
        cac_db_host, cac_db_port, cac_db_name, cac_db_username, encrypted_db_password,
        JSON.stringify(cac_sample_json), JSON.stringify(cac_field_mapping),
        cac_sync_interval_minutes || 5, cac_is_active !== false, cac_jsonordb
      ];

      const result = await pool.query(query, values);

      logger.info('API config created', { cac_config_id, cac_customer_id });
      res.status(201).json({ success: true, data: result.rows[0] });
    } catch (error) {
      logger.error('Failed to create API config', { error: error.message });
      res.status(500).json({ success: false, message: 'Failed to create API config', error: error.message });
    }
  }
);

// DELETE - Remove API configuration
router.delete('/configs/:config_id',
  [
    param('config_id').isUUID().withMessage('Invalid config ID format')
  ],
  validate,
  async (req, res) => {
    const { config_id } = req.params;

    try {
      const query = 'DELETE FROM customer_api_configs WHERE cac_config_id = $1 RETURNING cac_config_id, cac_customer_id, cac_outlet_id';
      const result = await pool.query(query, [config_id]);

      if (result.rows.length === 0) {
        logger.warn('Config not found', { config_id });
        return res.status(404).json({ success: false, message: 'Configuration not found' });
      }

      logger.info('API config deleted', { config_id });
      res.json({ success: true, message: 'Configuration deleted successfully', data: result.rows[0] });
    } catch (error) {
      logger.error('Failed to delete API config', { error: error.message });
      res.status(500).json({ success: false, message: 'Failed to delete configuration', error: error.message });
    }
  }
);

// GET - List all configurations
router.get('/configs', async (req, res) => {
  try {
    const query = `
      SELECT 
        cac_config_id, cac_customer_id, cac_outlet_id, cac_pos_vendor,
        cac_api_url, cac_http_method, cac_jsonordb, cac_sync_interval_minutes,
        cac_is_active, cac_created_at, cac_updated_at
      FROM customer_api_configs
      ORDER BY cac_created_at DESC
    `;
    const result = await pool.query(query);

    res.json({ success: true, data: result.rows, count: result.rows.length });
  } catch (error) {
    logger.error('Failed to fetch configs', { error: error.message });
    res.status(500).json({ success: false, message: 'Failed to fetch configurations', error: error.message });
  }
});

// GET - Get single configuration
router.get('/configs/:config_id',
  [
    param('config_id').isUUID().withMessage('Invalid config ID format')
  ],
  validate,
  async (req, res) => {
    const { config_id } = req.params;

    try {
      const query = `
        SELECT 
          cac_config_id, cac_customer_id, cac_outlet_id, cac_pos_vendor,
          cac_api_url, cac_http_method, cac_jsonordb, cac_sync_interval_minutes,
          cac_is_active, cac_created_at, cac_updated_at, cac_sample_json, cac_field_mapping
        FROM customer_api_configs
        WHERE cac_config_id = $1
      `;
      const result = await pool.query(query, [config_id]);

      if (result.rows.length === 0) {
        return res.status(404).json({ success: false, message: 'Configuration not found' });
      }

      res.json({ success: true, data: result.rows[0] });
    } catch (error) {
      logger.error('Failed to fetch config', { error: error.message });
      res.status(500).json({ success: false, message: 'Failed to fetch configuration', error: error.message });
    }
  }
);

module.exports = router;
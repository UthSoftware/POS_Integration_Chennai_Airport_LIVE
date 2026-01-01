const express = require('express');
const { body, param, validationResult } = require('express-validator');
const pool = require('../../config/database');
const createLogger = require('../../config/logger');
const { v4: uuidv4 } = require('uuid');

const router = express.Router();
const logger = createLogger('api-outlet');

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
// CUSTOMER OUTLET MAPPING ROUTES
// ============================================

// CREATE - Add new outlet mapping
router.post('/outlets',
  [
    body('com_customer_id').notEmpty().withMessage('Customer ID is required'),
    body('com_outlet_code').optional().isString(),
    body('com_outlet_id').optional().isUUID(),
    body('com_brand_id').optional().isUUID(),
    body('com_terminal').optional().isString(),
    body('com_gate').optional().isString(),
    body('brand_name').optional().isString(),
    body('com_is_active').optional().isBoolean()
  ],
  validate,
  async (req, res) => {
    const {
      com_customer_id, com_outlet_code, com_outlet_id, com_brand_id,
      com_terminal, com_gate, brand_name, com_is_active
    } = req.body;

    const com_id = uuidv4();

    try {
      const query = `
        INSERT INTO customer_outlet_mapping (
          com_id, com_customer_id, com_outlet_code, com_outlet_id, com_brand_id,
          com_terminal, com_gate, brand_name, com_is_active
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING *
      `;

      const values = [
        com_id, com_customer_id, com_outlet_code, com_outlet_id || null,
        com_brand_id || null, com_terminal, com_gate, brand_name, com_is_active !== false
      ];

      const result = await pool.query(query, values);

      logger.info('Outlet mapping created', { com_id, com_customer_id });
      res.status(201).json({ success: true, data: result.rows[0] });
    } catch (error) {
      logger.error('Failed to create outlet mapping', { error: error.message });
      res.status(500).json({ success: false, message: 'Failed to create outlet mapping', error: error.message });
    }
  }
);

// DELETE - Remove outlet mapping
router.delete('/outlets/:outlet_id',
  [
    param('outlet_id').isUUID().withMessage('Invalid outlet ID format')
  ],
  validate,
  async (req, res) => {
    const { outlet_id } = req.params;

    try {
      const query = 'DELETE FROM customer_outlet_mapping WHERE com_id = $1 RETURNING *';
      const result = await pool.query(query, [outlet_id]);

      if (result.rows.length === 0) {
        logger.warn('Outlet mapping not found', { outlet_id });
        return res.status(404).json({ success: false, message: 'Outlet mapping not found' });
      }

      logger.info('Outlet mapping deleted', { outlet_id });
      res.json({ success: true, message: 'Outlet mapping deleted successfully', data: result.rows[0] });
    } catch (error) {
      logger.error('Failed to delete outlet mapping', { error: error.message });
      res.status(500).json({ success: false, message: 'Failed to delete outlet mapping', error: error.message });
    }
  }
);

// GET - List all outlet mappings
router.get('/outlets', async (req, res) => {
  try {
    const query = 'SELECT * FROM customer_outlet_mapping ORDER BY com_created_at DESC';
    const result = await pool.query(query);

    res.json({ success: true, data: result.rows, count: result.rows.length });
  } catch (error) {
    logger.error('Failed to fetch outlet mappings', { error: error.message });
    res.status(500).json({ success: false, message: 'Failed to fetch outlet mappings', error: error.message });
  }
});

// GET - Get single outlet mapping
router.get('/outlets/:outlet_id',
  [
    param('outlet_id').isUUID().withMessage('Invalid outlet ID format')
  ],
  validate,
  async (req, res) => {
    const { outlet_id } = req.params;

    try {
      const query = 'SELECT * FROM customer_outlet_mapping WHERE com_id = $1';
      const result = await pool.query(query, [outlet_id]);

      if (result.rows.length === 0) {
        return res.status(404).json({ success: false, message: 'Outlet mapping not found' });
      }

      res.json({ success: true, data: result.rows[0] });
    } catch (error) {
      logger.error('Failed to fetch outlet mapping', { error: error.message });
      res.status(500).json({ success: false, message: 'Failed to fetch outlet mapping', error: error.message });
    }
  }
);

module.exports = router;
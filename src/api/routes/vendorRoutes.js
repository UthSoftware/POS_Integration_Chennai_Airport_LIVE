const express = require('express');
const { body, param, validationResult } = require('express-validator');
const pool = require('../../config/database');
const createLogger = require('../../config/logger');
const { v4: uuidv4 } = require('uuid');

const router = express.Router();
const logger = createLogger('api-vendor');

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
// POS VENDOR MASTER ROUTES
// ============================================

// CREATE - Add new vendor
router.post('/vendors',
  [
    body('vendor_name').notEmpty().withMessage('Vendor name is required'),
    body('contact_email').optional().isEmail().withMessage('Invalid email format'),
    body('contact_phone').optional().isString(),
    body('base_format').optional().isString(),
    body('remarks').optional().isString()
  ],
  validate,
  async (req, res) => {
    const { vendor_name, contact_email, contact_phone, base_format, remarks } = req.body;
    const vendor_id = uuidv4();

    try {
      const query = `
        INSERT INTO pos_vendor_master (vendor_id, vendor_name, contact_email, contact_phone, base_format, remarks)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING *
      `;
      
      const values = [vendor_id, vendor_name, contact_email, contact_phone, base_format, remarks];
      const result = await pool.query(query, values);

      logger.info('Vendor created', { vendor_id, vendor_name });
      res.status(201).json({ success: true, data: result.rows[0] });
    } catch (error) {
      logger.error('Failed to create vendor', { error: error.message });
      res.status(500).json({ success: false, message: 'Failed to create vendor', error: error.message });
    }
  }
);

// DELETE - Remove vendor
router.delete('/vendors/:vendor_id',
  [
    param('vendor_id').isUUID().withMessage('Invalid vendor ID format')
  ],
  validate,
  async (req, res) => {
    const { vendor_id } = req.params;

    try {
      const query = 'DELETE FROM pos_vendor_master WHERE vendor_id = $1 RETURNING *';
      const result = await pool.query(query, [vendor_id]);

      if (result.rows.length === 0) {
        logger.warn('Vendor not found', { vendor_id });
        return res.status(404).json({ success: false, message: 'Vendor not found' });
      }

      logger.info('Vendor deleted', { vendor_id });
      res.json({ success: true, message: 'Vendor deleted successfully', data: result.rows[0] });
    } catch (error) {
      logger.error('Failed to delete vendor', { error: error.message });
      res.status(500).json({ success: false, message: 'Failed to delete vendor', error: error.message });
    }
  }
);

// GET - List all vendors
router.get('/vendors', async (req, res) => {
  try {
    const query = 'SELECT * FROM pos_vendor_master ORDER BY created_at DESC';
    const result = await pool.query(query);

    res.json({ success: true, data: result.rows, count: result.rows.length });
  } catch (error) {
    logger.error('Failed to fetch vendors', { error: error.message });
    res.status(500).json({ success: false, message: 'Failed to fetch vendors', error: error.message });
  }
});

// GET - Get single vendor
router.get('/vendors/:vendor_id',
  [
    param('vendor_id').isUUID().withMessage('Invalid vendor ID format')
  ],
  validate,
  async (req, res) => {
    const { vendor_id } = req.params;

    try {
      const query = 'SELECT * FROM pos_vendor_master WHERE vendor_id = $1';
      const result = await pool.query(query, [vendor_id]);

      if (result.rows.length === 0) {
        return res.status(404).json({ success: false, message: 'Vendor not found' });
      }

      res.json({ success: true, data: result.rows[0] });
    } catch (error) {
      logger.error('Failed to fetch vendor', { error: error.message });
      res.status(500).json({ success: false, message: 'Failed to fetch vendor', error: error.message });
    }
  }
);

module.exports = router;
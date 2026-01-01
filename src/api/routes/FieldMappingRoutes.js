const express = require('express');
const { body, param, validationResult } = require('express-validator');
const pool = require('../../config/database');
const createLogger = require('../../config/logger');
const { v4: uuidv4 } = require('uuid');

const router = express.Router();
const logger = createLogger('api-field-mapping');

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
// POS VENDOR FIELD MAPPING ROUTES
// ============================================

// CREATE - Add new field mapping
router.post('/field-mappings',
  [
    body('pvfm_vendor_id').isUUID().withMessage('Vendor ID must be a valid UUID'),
    body('pvfm_source_field').optional().isString(),
    body('pvfm_target_field').notEmpty().withMessage('Target field is required'),
    body('pvfm_json_path').optional().isString(),
    body('pvfm_transform_rule').optional().isString(),
    body('pvfm_is_required').optional().isBoolean(),
    body('pvfm_tablename').notEmpty().withMessage('Table name is required')
  ],
  validate,
  async (req, res) => {
    const {
      pvfm_vendor_id, pvfm_source_field, pvfm_target_field, pvfm_json_path,
      pvfm_transform_rule, pvfm_is_required, pvfm_tablename
    } = req.body;

    const pvfm_mapping_id = uuidv4();

    try {
      const query = `
        INSERT INTO pos_vendor_field_mapping (
          pvfm_mapping_id, pvfm_vendor_id, pvfm_source_field, pvfm_target_field,
          pvfm_json_path, pvfm_transform_rule, pvfm_is_required, pvfm_tablename
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING *
      `;

      const values = [
        pvfm_mapping_id, pvfm_vendor_id, pvfm_source_field, pvfm_target_field,
        pvfm_json_path, pvfm_transform_rule, pvfm_is_required || false, pvfm_tablename
      ];

      const result = await pool.query(query, values);

      logger.info('Field mapping created', { pvfm_mapping_id, pvfm_vendor_id });
      res.status(201).json({ success: true, data: result.rows[0] });
    } catch (error) {
      logger.error('Failed to create field mapping', { error: error.message });
      res.status(500).json({ success: false, message: 'Failed to create field mapping', error: error.message });
    }
  }
);

// DELETE - Remove field mapping
router.delete('/field-mappings/:mapping_id',
  [
    param('mapping_id').isUUID().withMessage('Invalid mapping ID format')
  ],
  validate,
  async (req, res) => {
    const { mapping_id } = req.params;

    try {
      const query = 'DELETE FROM pos_vendor_field_mapping WHERE pvfm_mapping_id = $1 RETURNING *';
      const result = await pool.query(query, [mapping_id]);

      if (result.rows.length === 0) {
        logger.warn('Field mapping not found', { mapping_id });
        return res.status(404).json({ success: false, message: 'Field mapping not found' });
      }

      logger.info('Field mapping deleted', { mapping_id });
      res.json({ success: true, message: 'Field mapping deleted successfully', data: result.rows[0] });
    } catch (error) {
      logger.error('Failed to delete field mapping', { error: error.message });
      res.status(500).json({ success: false, message: 'Failed to delete field mapping', error: error.message });
    }
  }
);

// GET - List all field mappings
router.get('/field-mappings', async (req, res) => {
  const { vendor_id, tablename } = req.query;

  try {
    let query = 'SELECT * FROM pos_vendor_field_mapping WHERE 1=1';
    const values = [];

    if (vendor_id) {
      values.push(vendor_id);
      query += ` AND pvfm_vendor_id = $${values.length}`;
    }

    if (tablename) {
      values.push(tablename);
      query += ` AND pvfm_tablename = $${values.length}`;
    }

    query += ' ORDER BY pvfm_created_at DESC';

    const result = await pool.query(query, values);

    res.json({ success: true, data: result.rows, count: result.rows.length });
  } catch (error) {
    logger.error('Failed to fetch field mappings', { error: error.message });
    res.status(500).json({ success: false, message: 'Failed to fetch field mappings', error: error.message });
  }
});

// GET - Get single field mapping
router.get('/field-mappings/:mapping_id',
  [
    param('mapping_id').isUUID().withMessage('Invalid mapping ID format')
  ],
  validate,
  async (req, res) => {
    const { mapping_id } = req.params;

    try {
      const query = 'SELECT * FROM pos_vendor_field_mapping WHERE pvfm_mapping_id = $1';
      const result = await pool.query(query, [mapping_id]);

      if (result.rows.length === 0) {
        return res.status(404).json({ success: false, message: 'Field mapping not found' });
      }

      res.json({ success: true, data: result.rows[0] });
    } catch (error) {
      logger.error('Failed to fetch field mapping', { error: error.message });
      res.status(500).json({ success: false, message: 'Failed to fetch field mapping', error: error.message });
    }
  }
);

module.exports = router;
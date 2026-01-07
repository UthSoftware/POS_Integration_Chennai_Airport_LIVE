const pool = require('../config/database');
const { decrypt } = require('../utils/encryption');

class ConfigModel {
  static async getActiveConfigs(vendorFilter = null) {
    let query = `
      SELECT 
        cac.*,
        com.com_outlet_id,
        com.com_brand_id,
        com.com_terminal,
        com.com_gate,
        com.brand_name,
        pvm.vendor_name,
        pvm.vendor_id
      FROM customer_api_configs cac
      LEFT JOIN customer_outlet_mapping com 
        ON cac.cac_customer_id = com.com_customer_id 
        AND cac.cac_outlet_id = com.com_outlet_code
      LEFT JOIN pos_vendor_master pvm 
        ON cac.cac_customer_id = pvm.vendor_id::text
      WHERE cac.cac_is_active = true 
      and com.com_is_active = true
        
    `;
    // AND com.com_is_active = true
    const params = [];
    
    // Filter by vendor if provided
    if (vendorFilter && vendorFilter.length > 0) {
      query += ` AND UPPER(pvm.vendor_name) = ANY($1::text[])`;
      params.push(vendorFilter.map(v => v.toUpperCase()));
    }
    
    const result = await pool.query(query, params);
    
    // Decrypt sensitive fields
    return result.rows.map(config => ({
      ...config,
      cac_auth_header_value: config.cac_auth_header_value,
      cac_db_password: config.cac_db_password
    }));
  }

  static async getFieldMapping(vendorId, tableName = null) {
    let query = `
      SELECT * FROM pos_vendor_field_mapping
      WHERE pvfm_vendor_id = $1
    `;
    
    const params = [vendorId];
   
    
    if (tableName) {
      query += ` AND pvfm_tablename = $2`;
      params.push(tableName);
    }
    
    query += ` ORDER BY pvfm_tablename, pvfm_is_required DESC`;
    
    const result = await pool.query(query, params);
    return result.rows;
  }

  static async getAllFieldMappings(vendorId) {
    const query = `
      SELECT * FROM pos_vendor_field_mapping
      WHERE pvfm_vendor_id = $1
      ORDER BY pvfm_tablename, pvfm_is_required DESC
    `;
    //  console.log('Vendor ID for field mapping:', query);
    const result = await pool.query(query, [vendorId]);
    
    // Group by table name
    const mappings = {
      raw_transactions: [],
      raw_transaction_items: [],
      raw_payment: []
    };
    
    result.rows.forEach(row => {
      if (mappings[row.pvfm_tablename]) {
        mappings[row.pvfm_tablename].push(row);
      }
    });
    // console.log('Field mappings retrieved:', mappings);
    return mappings;
  }
}

module.exports = ConfigModel;
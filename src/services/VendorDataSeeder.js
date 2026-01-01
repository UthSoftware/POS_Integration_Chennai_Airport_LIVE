const fs = require('fs').promises;
const path = require('path');
const pool = require('../config/database');
const createLogger = require('../config/logger');
const { v4: uuidv4 } = require('uuid');
const { encrypt } = require('../utils/encryption');

class VendorDataSeeder {
  constructor() {
    this.logger = createLogger('vendor-seeder');
    this.vendorFilePath = path.join(__dirname, '../../vendor_details/vendordetails.txt');
  }

  async seedVendorData() {
    console.log('\n🌱 Starting Vendor Data Seeding Process...\n');
    this.logger.info('Starting vendor data seeding process');

    try {
      // Step 1: Read vendor details file
      const vendorData = await this.readVendorFile();
      
      if (!vendorData || vendorData.length === 0) {
        console.log('⚠️  No vendor data found in vendordetails.txt');
        this.logger.warn('No vendor data found in file');
        return { success: true, message: 'No vendor data to seed' };
      }

      console.log(`📄 Found ${vendorData.length} vendor configuration(s) in file\n`);
      this.logger.info(`Found ${vendorData.length} vendor configurations`);

      // Step 2: Process each vendor
      let insertedCount = 0;
      let existingCount = 0;
      let errorCount = 0;

      for (const vendor of vendorData) {
        try {
          const result = await this.processVendor(vendor);
          if (result.inserted) {
            insertedCount += result.insertedRecords;
          }
          if (result.existing) {
            existingCount++;
          }
        } catch (error) {
          errorCount++;
          this.logger.error('Failed to process vendor', { vendor, error: error.message });
          console.error(`❌ Failed to process vendor ${vendor.vendor_name}: ${error.message}`);
        }
      }

      // Step 3: Summary
      console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
      console.log('📊 Vendor Data Seeding Summary:');
      console.log(`   ✅ Vendors processed: ${vendorData.length}`);
      console.log(`   ➕ New records inserted: ${insertedCount}`);
      console.log(`   ✓ Existing vendors: ${existingCount}`);
      console.log(`   ❌ Errors: ${errorCount}`);
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

      this.logger.info('Vendor data seeding completed', {
        total: vendorData.length,
        inserted: insertedCount,
        existing: existingCount,
        errors: errorCount
      });

      return {
        success: errorCount === 0,
        total: vendorData.length,
        inserted: insertedCount,
        existing: existingCount,
        errors: errorCount
      };

    } catch (error) {
      this.logger.error('Vendor data seeding failed', { error: error.message, stack: error.stack });
      console.error('\n❌ Vendor data seeding failed:', error.message, '\n');
      throw error;
    }
  }

  async readVendorFile() {
    try {
      const fileContent = await fs.readFile(this.vendorFilePath, 'utf-8');
      const lines = fileContent.split('\n').filter(line => line.trim() && !line.startsWith('#'));
      
      const vendors = [];
      
      for (const line of lines) {
        const parts = line.split('|').map(p => p.trim());
        
        if (parts.length >= 3) {
          vendors.push({
            vendor_name: parts[0],
            outlet_name: parts[1],
            terminal_name: parts[2],
            customer_id: parts[3] || `CUST_${parts[0].replace(/\s+/g, '_').toUpperCase()}`,
            outlet_code: parts[4] || `OUTLET_${parts[1].replace(/\s+/g, '_').toUpperCase()}`,
            brand_name: parts[5] || parts[0],
            gate: parts[6] || 'GATE01',
            api_url: parts[7] || null,
            db_host: parts[8] || null,
            db_port: parts[9] ? parseInt(parts[9]) : null,
            db_name: parts[10] || null,
            source_type: parts[11] || 'api'
          });
        }
      }

      return vendors;
    } catch (error) {
      if (error.code === 'ENOENT') {
        this.logger.warn('Vendor details file not found', { path: this.vendorFilePath });
        console.log(`⚠️  Vendor details file not found: ${this.vendorFilePath}`);
        return [];
      }
      throw error;
    }
  }

  async processVendor(vendorData) {
    console.log(`\n🔍 Processing: ${vendorData.vendor_name} - ${vendorData.outlet_name} - ${vendorData.terminal_name}`);
    
    let insertedRecords = 0;
    let isExisting = true;

    // Step 1: Check and insert vendor master
    const vendorId = await this.ensureVendorMaster(vendorData);
    if (vendorId.inserted) {
      insertedRecords++;
      isExisting = false;
    }

    // Step 2: Check and insert outlet mapping
    const outletMappingId = await this.ensureOutletMapping(vendorData, vendorId.id);
    if (outletMappingId.inserted) {
      insertedRecords++;
      isExisting = false;
    }

    // Step 3: Check and insert API config
    const configId = await this.ensureApiConfig(vendorData, vendorId.id, outletMappingId.outlet_id, outletMappingId.brand_id);
    if (configId.inserted) {
      insertedRecords++;
      isExisting = false;
    }

    // Step 4: Check and insert field mappings (default mappings)
    const mappingsInserted = await this.ensureFieldMappings(vendorId.id);
    if (mappingsInserted > 0) {
      insertedRecords += mappingsInserted;
      isExisting = false;
    }

    if (isExisting) {
      console.log(`   ✓ All records already exist`);
    } else {
      console.log(`   ✅ Inserted ${insertedRecords} new record(s)`);
    }

    return { inserted: !isExisting, existing: isExisting, insertedRecords };
  }

  async ensureVendorMaster(vendorData) {
    const checkQuery = 'SELECT vendor_id FROM pos_vendor_master WHERE vendor_name = $1';
    const checkResult = await pool.query(checkQuery, [vendorData.vendor_name]);

    if (checkResult.rows.length > 0) {
      this.logger.debug('Vendor master exists', { vendor_name: vendorData.vendor_name });
      return { id: checkResult.rows[0].vendor_id, inserted: false };
    }

    // Insert new vendor
    const vendorId = uuidv4();
    const insertQuery = `
      INSERT INTO pos_vendor_master (vendor_id, vendor_name, contact_email, remarks)
      VALUES ($1, $2, $3, $4)
      RETURNING vendor_id
    `;
    
    await pool.query(insertQuery, [
      vendorId,
      vendorData.vendor_name,
      `contact@${vendorData.vendor_name.toLowerCase().replace(/\s+/g, '')}.com`,
      `Auto-seeded from vendor details file`
    ]);

    this.logger.info('Vendor master created', { vendor_id: vendorId, vendor_name: vendorData.vendor_name });
    console.log(`   ➕ Created vendor master: ${vendorData.vendor_name}`);
    
    return { id: vendorId, inserted: true };
  }

  async ensureOutletMapping(vendorData, vendorId) {
    const checkQuery = `
      SELECT com_id, com_outlet_id, com_brand_id 
      FROM customer_outlet_mapping 
      WHERE com_customer_id = $1 AND com_outlet_code = $2 AND com_terminal = $3
    `;
    const checkResult = await pool.query(checkQuery, [
      vendorData.customer_id,
      vendorData.outlet_code,
      vendorData.terminal_name
    ]);

    if (checkResult.rows.length > 0) {
      this.logger.debug('Outlet mapping exists', { outlet_code: vendorData.outlet_code });
      return {
        id: checkResult.rows[0].com_id,
        outlet_id: checkResult.rows[0].com_outlet_id,
        brand_id: checkResult.rows[0].com_brand_id,
        inserted: false
      };
    }

    // Insert new outlet mapping
    const mappingId = uuidv4();
    const outletId = uuidv4();
    const brandId = uuidv4();
    
    const insertQuery = `
      INSERT INTO customer_outlet_mapping (
        com_id, com_customer_id, com_outlet_code, com_outlet_id, com_brand_id,
        com_terminal, com_gate, brand_name, com_is_active
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
      RETURNING com_id, com_outlet_id, com_brand_id
    `;
    
    const result = await pool.query(insertQuery, [
      mappingId,
      vendorData.customer_id,
      vendorData.outlet_code,
      outletId,
      brandId,
      vendorData.terminal_name,
      vendorData.gate,
      vendorData.brand_name,
      true
    ]);

    this.logger.info('Outlet mapping created', { mapping_id: mappingId, outlet: vendorData.outlet_name });
    console.log(`   ➕ Created outlet mapping: ${vendorData.outlet_name}`);
    
    return {
      id: result.rows[0].com_id,
      outlet_id: result.rows[0].com_outlet_id,
      brand_id: result.rows[0].com_brand_id,
      inserted: true
    };
  }

  async ensureApiConfig(vendorData, vendorId, outletId, brandId) {
    const checkQuery = `
      SELECT cac_config_id 
      FROM customer_api_configs 
      WHERE cac_customer_id = $1 AND cac_outlet_id = $2 AND cac_pos_vendor = $3
    `;
    const checkResult = await pool.query(checkQuery, [
      vendorData.customer_id,
      vendorData.outlet_code,
      vendorId
    ]);

    if (checkResult.rows.length > 0) {
      this.logger.debug('API config exists', { config_id: checkResult.rows[0].cac_config_id });
      return { id: checkResult.rows[0].cac_config_id, inserted: false };
    }

    // Insert new API config
    const configId = uuidv4();
    const sourceType = vendorData.source_type.toLowerCase();
    
    const insertQuery = `
      INSERT INTO customer_api_configs (
        cac_config_id, cac_customer_id, cac_outlet_id, cac_pos_vendor,
        cac_api_url, cac_http_method, cac_auth_type, cac_db_host, cac_db_port,
        cac_db_name, cac_sync_interval_minutes, cac_is_active, cac_jsonordb
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
      RETURNING cac_config_id
    `;
    
    await pool.query(insertQuery, [
      configId,
      vendorData.customer_id,
      vendorData.outlet_code,
      vendorId,
      vendorData.api_url,
      'GET',
      'Bearer',
      vendorData.db_host,
      vendorData.db_port,
      vendorData.db_name,
      5,
      true,
      sourceType
    ]);

    this.logger.info('API config created', { config_id: configId, source_type: sourceType });
    console.log(`   ➕ Created API config (${sourceType})`);
    
    return { id: configId, inserted: true };
  }

  async ensureFieldMappings(vendorId) {
    // Check if any field mappings exist for this vendor
    const checkQuery = 'SELECT COUNT(*) as count FROM pos_vendor_field_mapping WHERE pvfm_vendor_id = $1';
    const checkResult = await pool.query(checkQuery, [vendorId]);
    
    if (parseInt(checkResult.rows[0].count) > 0) {
      this.logger.debug('Field mappings exist', { vendor_id: vendorId });
      return 0;
    }

    // Insert default field mappings for transactions
    const defaultMappings = [
      {
        source_field: 'transaction_id',
        target_field: 'source_transaction_ref',
        json_path: 'transaction.id',
        tablename: 'raw_transactions',
        is_required: true
      },
      {
        source_field: 'transaction_time',
        target_field: 'transaction_time',
        json_path: 'transaction.timestamp',
        tablename: 'raw_transactions',
        is_required: true
      },
      {
        source_field: 'net_amount',
        target_field: 'net_amount',
        json_path: 'transaction.total',
        tablename: 'raw_transactions',
        is_required: true
      },
      {
        source_field: 'transaction_type',
        target_field: 'transaction_type',
        json_path: 'transaction.type',
        tablename: 'raw_transactions',
        is_required: false
      }
    ];

    let insertedCount = 0;
    
    for (const mapping of defaultMappings) {
      const mappingId = uuidv4();
      const insertQuery = `
        INSERT INTO pos_vendor_field_mapping (
          pvfm_mapping_id, pvfm_vendor_id, pvfm_source_field, pvfm_target_field,
          pvfm_json_path, pvfm_tablename, pvfm_is_required
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
      `;
      
      await pool.query(insertQuery, [
        mappingId,
        vendorId,
        mapping.source_field,
        mapping.target_field,
        mapping.json_path,
        mapping.tablename,
        mapping.is_required
      ]);
      
      insertedCount++;
    }

    this.logger.info('Field mappings created', { vendor_id: vendorId, count: insertedCount });
    console.log(`   ➕ Created ${insertedCount} default field mapping(s)`);
    
    return insertedCount;
  }
}

module.exports = VendorDataSeeder;
const pool = require('../config/database');
const createLogger = require('../config/logger');

class ConfigValidator {
  constructor() {
    this.logger = createLogger('config-validator');
  }

  async validateRequiredTables() {
    this.logger.info('Starting configuration validation...');
    
    const validationResults = {
      isValid: true,
      missingTables: [],
      tableCounts: {}
    };

    try {
      // Check pos_vendor_master
      const vendorCount = await this.checkTable('pos_vendor_master', 'vendor_id');
      validationResults.tableCounts.pos_vendor_master = vendorCount;
      if (vendorCount === 0) {
        validationResults.isValid = false;
        validationResults.missingTables.push('pos_vendor_master');
      }

      // Check customer_api_configs
      const configCount = await this.checkTable('customer_api_configs', 'cac_config_id');
      validationResults.tableCounts.customer_api_configs = configCount;
      if (configCount === 0) {
        validationResults.isValid = false;
        validationResults.missingTables.push('customer_api_configs');
      }

      // Check customer_outlet_mapping
      const outletCount = await this.checkTable('customer_outlet_mapping', 'com_id');
      validationResults.tableCounts.customer_outlet_mapping = outletCount;
      if (outletCount === 0) {
        validationResults.isValid = false;
        validationResults.missingTables.push('customer_outlet_mapping');
      }

      // Check pos_vendor_field_mapping
      const mappingCount = await this.checkTable('pos_vendor_field_mapping', 'pvfm_mapping_id');
      validationResults.tableCounts.pos_vendor_field_mapping = mappingCount;
      if (mappingCount === 0) {
        validationResults.isValid = false;
        validationResults.missingTables.push('pos_vendor_field_mapping');
      }

      // Log results
      if (validationResults.isValid) {
        this.logger.info('Configuration validation passed', validationResults.tableCounts);
        console.log('\n✅ Configuration Validation: PASSED');
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log(`📊 Table Record Counts:`);
        console.log(`   • pos_vendor_master: ${validationResults.tableCounts.pos_vendor_master} records`);
        console.log(`   • customer_api_configs: ${validationResults.tableCounts.customer_api_configs} records`);
        console.log(`   • customer_outlet_mapping: ${validationResults.tableCounts.customer_outlet_mapping} records`);
        console.log(`   • pos_vendor_field_mapping: ${validationResults.tableCounts.pos_vendor_field_mapping} records`);
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
      } else {
        this.logger.warn('Configuration validation failed', {
          missingTables: validationResults.missingTables,
          tableCounts: validationResults.tableCounts
        });
        console.log('\n❌ Configuration Validation: FAILED');
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
        console.log('⚠️  The following tables have NO records:');
        validationResults.missingTables.forEach(table => {
          console.log(`   ✗ ${table}`);
        });
        console.log('\n📋 Current Record Counts:');
        Object.entries(validationResults.tableCounts).forEach(([table, count]) => {
          const status = count > 0 ? '✓' : '✗';
          console.log(`   ${status} ${table}: ${count} records`);
        });
        console.log('\n💡 Action Required:');
        console.log('   1. Use the REST API to create required configuration data:');
        console.log('      • POST /api/vendors - Create POS vendors');
        console.log('      • POST /api/configs - Create API/DB configurations');
        console.log('      • POST /api/outlets - Create outlet mappings');
        console.log('      • POST /api/field-mappings - Create field mappings');
        console.log('\n   2. Or manually insert data into the database tables');
        console.log('\n   3. Restart the application after adding data');
        console.log('\n🔗 API Server: http://localhost:3000');
        console.log('📚 API Documentation: See API_DOCUMENTATION.md');
        console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
      }

      return validationResults;

    } catch (error) {
      this.logger.error('Configuration validation error', { error: error.message, stack: error.stack });
      console.error('\n❌ Configuration Validation Error:', error.message);
      console.error('Please check database connection and table existence.\n');
      
      validationResults.isValid = false;
      validationResults.error = error.message;
      return validationResults;
    }
  }

  async checkTable(tableName, idColumn) {
    try {
      const query = `SELECT COUNT(${idColumn}) as count FROM ${tableName}`;
      const result = await pool.query(query);
      return parseInt(result.rows[0].count);
    } catch (error) {
      this.logger.error(`Failed to check table ${tableName}`, { error: error.message });
      throw new Error(`Failed to check table ${tableName}: ${error.message}`);
    }
  }

  async checkActiveConfigurations() {
    try {
      const query = `
        SELECT COUNT(*) as count 
        FROM customer_api_configs 
        WHERE cac_is_active = true
      `;
      const result = await pool.query(query);
      const activeCount = parseInt(result.rows[0].count);

      if (activeCount === 0) {
        this.logger.warn('No active configurations found');
        console.log('\n⚠️  Warning: No ACTIVE configurations found in customer_api_configs');
        console.log('   All configurations have cac_is_active = false');
        console.log('   The cron job will run but may not process any data.\n');
      }

      return activeCount;
    } catch (error) {
      this.logger.error('Failed to check active configurations', { error: error.message });
      return 0;
    }
  }
}

module.exports = ConfigValidator;
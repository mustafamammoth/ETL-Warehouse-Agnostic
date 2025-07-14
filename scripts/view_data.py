# ===== scripts/view_data.py (UPDATED for your structure) =====
"""
View extracted data
"""
import sys
import os
# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from extractors.utils.database import db
from utils.get_logger import get_logger

logger = get_logger(__name__)

def view_customers():
    logger.info("📊 Viewing customer data...")
    
    try:
        # Count customers
        count_query = "SELECT COUNT(*) as count FROM raw_data.acumatica_customers"
        count_result = db.read_query(count_query)
        total_customers = count_result.iloc[0]['count'] if not count_result.empty else 0
        
        logger.info(f"Total customers in database: {total_customers}")
        
        if total_customers > 0:
            # Show sample data
            sample_query = """
            SELECT 
                _extracted_at,
                _source_system,
                jsonb_pretty(_raw_data::jsonb) as sample_data
            FROM raw_data.acumatica_customers 
            LIMIT 1
            """
            
            sample_result = db.read_query(sample_query)
            if not sample_result.empty:
                logger.info("📝 Sample customer record:")
                print(sample_result.iloc[0]['sample_data'])
        else:
            logger.info("ℹ️ No customer data found. Run extraction first.")
            
    except Exception as e:
        logger.error(f"❌ Error viewing data: {e}")

def main():
    logger.info("🔍 Viewing extracted data...")
    view_customers()

if __name__ == "__main__":
    main()
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from config.warehouse_config import settings
from utils.get_logger import get_logger

logger = get_logger(__name__)

class DatabaseManager:
    def __init__(self):
        self._engine: Optional[Engine] = None
    
    @property
    def engine(self) -> Engine:
        if self._engine is None:
            logger.info(f"Creating database connection to {settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}")
            self._engine = create_engine(settings.database_url, echo=False)
        return self._engine
    
    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ Database connection successful")
            return True
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            return False
    
    def create_schema_if_not_exists(self, schema_name: str) -> None:
        try:
            with self.engine.connect() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
                conn.commit()
            logger.info(f"Schema {schema_name} created/verified")
        except Exception as e:
            logger.error(f"Failed to create schema {schema_name}: {e}")
            raise
    
    def write_dataframe(self, df: pd.DataFrame, table_name: str, schema: str = "raw_data", if_exists: str = "replace") -> None:
        try:
            self.create_schema_if_not_exists(schema)
            
            df.to_sql(
                table_name,
                self.engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                method="multi"
            )
            logger.info(f"✅ Successfully wrote {len(df)} rows to {schema}.{table_name}")
            
        except Exception as e:
            logger.error(f"❌ Failed to write DataFrame to {schema}.{table_name}: {e}")
            raise
    
    def read_query(self, query: str) -> pd.DataFrame:
        try:
            df = pd.read_sql(query, self.engine)
            logger.info(f"Query executed successfully, returned {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Query failed: {e}")
            raise

db = DatabaseManager()
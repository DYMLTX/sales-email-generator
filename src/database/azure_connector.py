"""Azure SQL Database connector for MAX.Live prospect data."""

import pyodbc
import pandas as pd
from typing import Optional, Dict, Any, List
from contextlib import contextmanager
import logging
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

from src.config.settings import settings

logger = logging.getLogger(__name__)


class AzureDBConnector:
    """Manages connections to Azure SQL Database."""
    
    def __init__(self):
        self.connection_string = settings.azure.connection_string
        self.engine = None
        
    def _create_sqlalchemy_engine(self):
        """Create SQLAlchemy engine for pandas operations."""
        # Build SQLAlchemy connection string
        if settings.azure.use_azure_ad:
            conn_str = (
                f"mssql+pyodbc:///?odbc_connect="
                f"{quote_plus(self.connection_string)}"
            )
        else:
            conn_str = (
                f"mssql+pyodbc://{settings.azure.username}:"
                f"{settings.azure.password}@{settings.azure.server}/"
                f"{settings.azure.database}?driver={quote_plus(settings.azure.driver)}"
            )
        
        return create_engine(conn_str, echo=False)
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = pyodbc.connect(self.connection_string)
            logger.info("Successfully connected to Azure SQL Database")
            yield conn
        except Exception as e:
            logger.error(f"Failed to connect to Azure SQL Database: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def test_connection(self) -> bool:
        """Test the database connection."""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                logger.info("Connection test successful")
                return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def get_table_schema(self, table_name: str) -> pd.DataFrame:
        """Get schema information for a specific table."""
        query = """
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            IS_NULLABLE,
            COLUMN_DEFAULT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """
        
        with self.get_connection() as conn:
            return pd.read_sql(query, conn, params=[table_name])
    
    def list_tables(self) -> List[str]:
        """List all tables in the database."""
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_TYPE = 'BASE TABLE'
        ORDER BY TABLE_NAME
        """
        
        with self.get_connection() as conn:
            df = pd.read_sql(query, conn)
            return df['TABLE_NAME'].tolist()
    
    def query_to_dataframe(self, query: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Execute a query and return results as a pandas DataFrame."""
        try:
            if not self.engine:
                self.engine = self._create_sqlalchemy_engine()
            
            return pd.read_sql(query, self.engine, params=params)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def get_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0]
    
    def sample_data(self, table_name: str, n: int = 10) -> pd.DataFrame:
        """Get a sample of data from a table."""
        query = f"SELECT TOP {n} * FROM {table_name}"
        return self.query_to_dataframe(query)


class ProspectDataAccess:
    """Specialized class for accessing prospect data."""
    
    def __init__(self):
        self.db = AzureDBConnector()
    
    def get_contacts(self, 
                    limit: Optional[int] = None,
                    filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Retrieve contacts from the database."""
        query = "SELECT * FROM contacts"
        conditions = []
        
        if filters:
            for key, value in filters.items():
                conditions.append(f"{key} = '{value}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        if limit:
            query += f" TOP {limit}"
        
        return self.db.query_to_dataframe(query)
    
    def get_accounts(self, 
                    limit: Optional[int] = None,
                    filters: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """Retrieve accounts from the database."""
        query = "SELECT * FROM accounts"
        conditions = []
        
        if filters:
            for key, value in filters.items():
                conditions.append(f"{key} = '{value}'")
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        if limit:
            query += f" TOP {limit}"
        
        return self.db.query_to_dataframe(query)
    
    def get_contacts_with_accounts(self) -> pd.DataFrame:
        """Get contacts joined with their account information."""
        query = """
        SELECT 
            c.*,
            a.name as account_name,
            a.industry,
            a.employee_count,
            a.annual_revenue
        FROM contacts c
        LEFT JOIN accounts a ON c.account_id = a.id
        WHERE c.email IS NOT NULL
        AND c.email != ''
        """
        
        return self.db.query_to_dataframe(query)
    
    def analyze_data_quality(self) -> Dict[str, Any]:
        """Analyze the quality of prospect data."""
        analysis = {}
        
        # Contacts analysis
        contacts_query = """
        SELECT 
            COUNT(*) as total_contacts,
            COUNT(DISTINCT email) as unique_emails,
            COUNT(CASE WHEN email IS NOT NULL AND email != '' THEN 1 END) as with_email,
            COUNT(CASE WHEN phone IS NOT NULL AND phone != '' THEN 1 END) as with_phone,
            COUNT(CASE WHEN account_id IS NOT NULL THEN 1 END) as with_account
        FROM contacts
        """
        
        # Accounts analysis  
        accounts_query = """
        SELECT 
            COUNT(*) as total_accounts,
            COUNT(CASE WHEN industry IS NOT NULL AND industry != '' THEN 1 END) as with_industry,
            COUNT(CASE WHEN employee_count IS NOT NULL THEN 1 END) as with_employee_count,
            COUNT(CASE WHEN annual_revenue IS NOT NULL THEN 1 END) as with_revenue
        FROM accounts
        """
        
        with self.db.get_connection() as conn:
            analysis['contacts'] = pd.read_sql(contacts_query, conn).to_dict('records')[0]
            analysis['accounts'] = pd.read_sql(accounts_query, conn).to_dict('records')[0]
        
        return analysis
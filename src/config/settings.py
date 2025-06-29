"""Configuration settings for MAX.Live Email Automation System."""

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class AzureSettings(BaseSettings):
    """Azure Database connection settings."""
    
    server: str
    database: str
    username: Optional[str] = None
    password: Optional[str] = None
    driver: str = "{ODBC Driver 18 for SQL Server}"
    use_azure_ad: bool = False
    connection_timeout: int = 30
    
    @property
    def connection_string(self) -> str:
        """Build Azure SQL connection string."""
        if self.use_azure_ad:
            return (
                f"Driver={self.driver};"
                f"Server=tcp:{self.server},1433;"
                f"Database={self.database};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout={self.connection_timeout};"
                f"Authentication=ActiveDirectoryDefault"
            )
        else:
            return (
                f"Driver={self.driver};"
                f"Server=tcp:{self.server},1433;"
                f"Database={self.database};"
                f"Uid={self.username};"
                f"Pwd={self.password};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout={self.connection_timeout}"
            )
    
    model_config = SettingsConfigDict(env_prefix="AZURE_DB_")


class GoogleCloudSettings(BaseSettings):
    """Google Cloud configuration."""
    
    project_id: str
    bigquery_dataset: str = "maxlive_prospects"
    storage_bucket: str = "maxlive-data-pipeline"
    location: str = "us-central1"
    
    model_config = SettingsConfigDict(env_prefix="GCP_")


class HubSpotSettings(BaseSettings):
    """HubSpot API configuration."""
    
    access_token: str
    api_base_url: str = "https://api.hubapi.com"
    
    model_config = SettingsConfigDict(env_prefix="HUBSPOT_")


class Settings(BaseSettings):
    """Main application settings."""
    
    app_name: str = "MAX.Live Email Automation"
    environment: str = "development"
    log_level: str = "INFO"
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"  # Ignore extra fields from .env
    )
    
    @property
    def azure(self) -> AzureSettings:
        return AzureSettings()
    
    @property
    def gcp(self) -> GoogleCloudSettings:
        return GoogleCloudSettings()
    
    @property
    def hubspot(self) -> HubSpotSettings:
        return HubSpotSettings()


# Global settings instance
settings = Settings()
from pydantic_settings import BaseSettings
from pydantic import Field, ConfigDict
from pathlib import Path
from typing import Optional

class Settings(BaseSettings):
    """
    Represents the application settings, loaded from environment variables.
    """
    processor_mode: str = Field("real", description="Processing mode: 'real' or 'simulator'.")
    db_path: Path = Field("consultas_goes.db", description="Path to the SQLite database file.")
    source_path: Path = Field("/depot/goes16", description="Root path of the primary storage (e.g., Lustre).")
    download_path: Path = Field("/data/tmp", description="Directory for query downloads.")
    max_workers: int = Field(8, description="Number of parallel I/O workers.")
    s3_fallback_enabled: bool = Field(True, description="Enable/disable fallback to S3.")
    lustre_enabled: bool = Field(True, description="Enable/disable the use of Lustre.")
    file_processing_timeout_seconds: int = Field(120, description="Maximum processing time per file in seconds.")
    sim_local_success_rate: float = Field(0.8, ge=0.0, le=1.0, description="Local success rate in simulator mode.")
    sim_s3_success_rate: float = Field(0.5, ge=0.0, le=1.0, description="S3 success rate in simulator mode.")
    max_files_per_query: int = Field(0, description="Maximum estimated files per query (0 = no limit).")
    max_size_mb_per_query: int = Field(0, description="Maximum estimated size in MB per query (0 = no limit).")
    min_free_space_gb_buffer: int = Field(10, description="Safety buffer in GB to leave free on disk.")
    api_key: Optional[str] = Field(None, description="Optional API Key for securing the endpoints.")

    # S3 Specific Settings
    S3_RETRY_ATTEMPTS: int = Field(3, description="Number of retry attempts for S3 operations.")
    S3_RETRY_BACKOFF_SECONDS: float = Field(1.0, description="Backoff factor for S3 retries in seconds.")
    S3_CONNECT_TIMEOUT: int = Field(5, description="S3 connection timeout in seconds.")
    S3_READ_TIMEOUT: int = Field(30, description="S3 read timeout in seconds.")
    S3_PROGRESS_STEP: int = Field(100, description="Update progress every N files for S3 downloads.")

    model_config = ConfigDict(env_file=".env", env_file_encoding='utf-8')

# Create a single, reusable instance of the settings
settings = Settings()

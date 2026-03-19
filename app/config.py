import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    zsxq_access_token: str
    group_id: str
    sqlite_db_path: str
    docs_storage_path: str
    sync_interval_seconds: int
    request_delay_seconds: float


def get_settings() -> Settings:
    return Settings(
        zsxq_access_token=os.getenv("ZSXQ_ACCESS_TOKEN", ""),
        group_id=os.getenv("GROUP_ID", ""),
        sqlite_db_path=os.getenv("SQLITE_DB_PATH", "data/openclaw.db"),
        docs_storage_path=os.getenv("DOCS_STORAGE_PATH", "data/docs"),
        sync_interval_seconds=int(os.getenv("SYNC_INTERVAL_SECONDS", "3600")),
        request_delay_seconds=float(os.getenv("REQUEST_DELAY_SECONDS", "0.5")),
    )

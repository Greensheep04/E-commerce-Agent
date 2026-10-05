from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DATA_CENTER_", extra="forbid")

    database_url: str = "postgresql+psycopg://data_center_writer@127.0.0.1:55432/shop_data_center"
    shop_url: str = "http://127.0.0.1:8002"
    service_token_file: Path = Path(".local/auth/data-center.service.key")
    artifact_root: Path = Path(".local/data-center")
    model: Literal["deepseek-flash", "qwen3.8-flash"] = "deepseek-flash"
    graph_enabled: bool = False
    graph_model: Literal["deepseek-flash", "qwen3.8-flash"] = "qwen3.8-flash"
    docling_models: Path = Path(".local/data-center/models/docling")
    tokenizer_cache: Path = Path(".local/data-center/tiktoken")
    tesseract_command: str = "tesseract"
    frontend_dir: Path = Path("frontend/dist")

    def get_service_token(self) -> str:
        token = self.service_token_file.read_text(encoding="utf-8").strip()
        if len(token) < 32 or not token.isascii():
            raise ValueError("数据服务身份凭据需要至少32个ASCII字符")
        return token

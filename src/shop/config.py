from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SHOP_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://shop@127.0.0.1:55432/shop"
    data_dir: Path = Path(".local/data")
    session_secret: str | None = None
    session_secret_file: Path = Path(".local/auth/session.key")
    admin_password_file: Path = Path(".local/auth/admin.password")
    upload_max_bytes: int = 32 * 1024 * 1024
    provider_env_file: Path | None = None
    dashscope_base_url: str | None = None
    harness_token_limits_file: Path = Path(".local/harness/token-limits.json")
    harness_frontend_dir: Path = Path("frontend/dist")
    poster_dir: Path = Path(".local/poster")
    assessment_order_adapter: str | None = None
    assessment_order_database_url: str | None = None
    assessment_order_source_file: Path | None = None
    cluster_config_file: Path | None = None
    cluster_notes_dir: Path = Path(".local/cluster/notes")
    data_center_url: str | None = None
    data_center_source_root: Path = Path(".local/data-center/sources")
    data_center_service_token_file: Path = Path(".local/auth/data-center.service.key")

    def get_session_secret(self) -> str:
        if self.session_secret is not None:
            return self.session_secret
        return self.session_secret_file.read_text(encoding="utf-8").strip()

    def get_data_center_service_token(self) -> str:
        token = self.data_center_service_token_file.read_text(encoding="utf-8").strip()
        if len(token) < 32 or not token.isascii():
            raise ValueError("数据服务身份凭据需要至少32个ASCII字符")
        return token

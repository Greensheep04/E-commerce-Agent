from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.config import Settings
from shop.fashion import run_import
from shop.models import User


def main():
    settings = Settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    with Session(engine) as session:
        owner = session.scalar(select(User).where(User.username == "admin", User.role == "admin"))
        if owner is None:
            raise ValueError("请先运行 init-admin")
        owner_id = owner.id
    batch = run_import(settings, owner_id)
    print(f"导入批次 {batch.id}：{batch.status}，记录 {batch.imported_count}")


if __name__ == "__main__":
    main()

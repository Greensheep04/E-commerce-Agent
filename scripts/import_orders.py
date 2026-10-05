import argparse
import json
import os
import secrets
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.adventureworks import REVISION, download_sources, import_orders
from shop.auth import hash_password
from shop.config import Settings
from shop.models import User
from shop.orders import bind_customer


def create_customer(session: Session, username: str, customer_id: int, directory: Path) -> None:
    user = session.scalar(select(User).where(User.username == username))
    password_file = directory / f"{username}.password"
    if user is None:
        if password_file.exists():
            raise FileExistsError("已有密码文件，但账号不存在")
        directory.mkdir(parents=True, exist_ok=True)
        password = secrets.token_urlsafe(32)
        user = User(username=username, role="customer", password_hash=hash_password(password))
        session.add(user)
        session.flush()
        descriptor = os.open(password_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(password)
        session.commit()
    bind_customer(session, user.id, customer_id)


def main():
    parser = argparse.ArgumentParser(description="导入固定版本的官方订单样例")
    parser.add_argument("--source-dir", type=Path, default=Path(".local/data/orders") / REVISION)
    parser.add_argument("--create-customers", action="store_true")
    args = parser.parse_args()
    manifest = download_sources(args.source_dir)
    with Session(create_engine(Settings().database_url)) as session:
        result = import_orders(session, args.source_dir)
        if args.create_customers:
            for username, customer_id in (("customer-11091", 11091), ("customer-11176", 11176)):
                create_customer(session, username, customer_id, Path(".local/auth"))
    print(json.dumps({**result, "manifest": manifest}, ensure_ascii=False))


if __name__ == "__main__":
    main()

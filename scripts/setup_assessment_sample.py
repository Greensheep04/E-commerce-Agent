import argparse
import json
import secrets
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from scripts.manage import write_secret
from shop.assessment_service import configure_access, import_employee_source
from shop.assessment_types import SAMPLE_NOTICE
from shop.auth import hash_password
from shop.config import Settings
from shop.models import User

SAMPLE_ASSIGNMENTS = (
    ("assessment-employee", "employee", 275, []),
    ("assessment-empty", "employee", 288, []),
    ("assessment-manager", "manager", 285, [275, 277, 278]),
    ("assessment-manager2", "manager", 286, [276, 279, 280]),
    ("assessment-admin", "admin", None, []),
)


def main():
    argparse.ArgumentParser().parse_args()
    settings = Settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    maker = sessionmaker(engine, expire_on_commit=False)
    directory = Path(".local/assessment/auth")
    manifest_path = directory / "sample-access.json"
    if manifest_path.exists():
        raise FileExistsError("样例权限配置已经存在，请使用管理接口读取或修改")
    entries = []
    try:
        with maker.begin() as session:
            source = import_employee_source(session, Path(".local/assessment/source"))
            for username, level, employee_id, managed in SAMPLE_ASSIGNMENTS:
                if session.scalar(select(User).where(User.username == username)):
                    raise ValueError(f"样例账号已存在：{username}")
                password_path = directory / f"{username}.password"
                password = secrets.token_urlsafe(32)
                user = User(username=username, password_hash=hash_password(password), role="viewer")
                session.add(user)
                session.flush()
                configure_access(session, user_id=user.id, level=level, employee_id=employee_id,
                                 managed_employee_ids=managed)
                write_secret(password_path, password)
                entries.append({"username": username, "user_id": user.id, "level": level,
                                "employee_id": employee_id, "managed_employee_ids": managed,
                                "password_file": str(password_path)})
        manifest_path.write_text(json.dumps({
            "notice": SAMPLE_NOTICE,
            "permission_notice": "此团队范围为本地验收的明确配置，不代表来源公司组织关系。",
            "source_fingerprint": source["fingerprint"], "accounts": entries,
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({"accounts": len(entries), "manifest": str(manifest_path)}, ensure_ascii=False))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()

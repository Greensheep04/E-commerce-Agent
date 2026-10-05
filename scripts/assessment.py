import argparse
import json
import secrets
from pathlib import Path

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from scripts.manage import write_secret
from shop.assessment import AssessmentService
from shop.assessment_orders import configured_order_port
from shop.assessment_service import SalesMetricService, configure_access, import_employee_source
from shop.auth import hash_password
from shop.config import Settings
from shop.models import User


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    source = commands.add_parser("import-employees")
    source.add_argument("--directory", type=Path, default=Path(".local/assessment/source"))
    create = commands.add_parser("create-user")
    create.add_argument("username")
    create.add_argument("--password-file", type=Path, required=True)
    binding = commands.add_parser("bind")
    binding.add_argument("username")
    binding.add_argument("--level", choices=("employee", "manager", "admin"), required=True)
    binding.add_argument("--employee-id", type=int)
    binding.add_argument("--managed-employee-ids", nargs="*", type=int, default=[])
    for name in ("create", "show", "run", "cancel", "export"):
        command = commands.add_parser(name)
        command.add_argument("--username", required=True)
        if name == "create":
            command.add_argument("prompt")
            command.add_argument("--parent-id")
        else:
            command.add_argument("id")
        if name == "export":
            command.add_argument("--format", choices=("html", "csv"), default="html")
            command.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    settings = Settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    maker = sessionmaker(engine, expire_on_commit=False)
    try:
        if args.command == "import-employees":
            with maker.begin() as session:
                result = import_employee_source(session, args.directory)
        elif args.command == "create-user":
            if args.password_file.exists():
                raise FileExistsError("账号密码文件已存在")
            password = secrets.token_urlsafe(32)
            with maker.begin() as session:
                if session.scalar(select(User).where(User.username == args.username)):
                    raise ValueError("账号名称已存在")
                user = User(username=args.username, password_hash=hash_password(password), role="viewer")
                session.add(user)
                session.flush()
                write_secret(args.password_file, password)
                result = {"username": args.username, "password_file": str(args.password_file)}
        elif args.command == "bind":
            with maker.begin() as session:
                user = session.scalar(select(User).where(User.username == args.username))
                if user is None:
                    raise ValueError("账号不存在")
                configure_access(session, user_id=user.id, level=args.level,
                                 employee_id=args.employee_id,
                                 managed_employee_ids=args.managed_employee_ids)
                result = {"username": args.username, "level": args.level,
                          "employee_id": args.employee_id, "managed_employee_ids": args.managed_employee_ids}
        else:
            with maker() as session:
                user = session.scalar(select(User).where(User.username == args.username))
                if user is None:
                    raise ValueError("账号不存在")
            service = AssessmentService(settings, SalesMetricService(configured_order_port(settings)))
            try:
                if args.command == "create":
                    result = service.create(args.prompt, user.id, args.parent_id)
                elif args.command == "show":
                    result = service.show(args.id, user.id)
                elif args.command == "run":
                    result = service.execute(args.id, user.id)
                elif args.command == "cancel":
                    result = service.cancel(args.id, user.id)
                else:
                    args.output.parent.mkdir(parents=True, exist_ok=True)
                    with args.output.open("x", encoding="utf-8", newline="") as output:
                        output.write(service.export(args.id, user.id, args.format))
                    result = {"export": str(args.output)}
            finally:
                service.close()
        print(json.dumps(result, ensure_ascii=False))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()

from pathlib import Path

import pytest

from shop.assessment_service import configure_access, import_employee_source
from shop.auth import hash_password
from shop.models import User

SOURCE_DIRECTORY = Path(".local/assessment/source")


@pytest.fixture
def assessment_accounts(db_session):
    import_employee_source(db_session, SOURCE_DIRECTORY)
    accounts = {}
    for name, employee_id, level, team in (
        ("employee", 275, "employee", []),
        ("other", 276, "employee", []),
        ("manager", 285, "manager", [275]),
        ("manager2", 286, "manager", [276]),
        ("admin", None, "admin", []),
    ):
        user = User(username=name, password_hash=hash_password("assessment-test-password"), role="viewer")
        db_session.add(user)
        db_session.flush()
        configure_access(db_session, user_id=user.id, level=level,
                         employee_id=employee_id, managed_employee_ids=team)
        accounts[name] = user
    db_session.commit()
    return accounts

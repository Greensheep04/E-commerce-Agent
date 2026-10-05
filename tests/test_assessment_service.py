import pytest
from sqlalchemy import func, select

from shop.assessment_models import AssessmentEmployee, AssessmentQuota, AssessmentRun
from shop.assessment_orders import DataBlocked
from shop.assessment_service import (
    AccessDenied,
    SalesMetricService,
    assert_run_read,
    configure_access,
    import_employee_source,
    principal_for,
)
from tests.assessment_fixtures import SOURCE_DIRECTORY


def test_official_employee_import_is_complete_and_idempotent(db_session):
    first = import_employee_source(db_session, SOURCE_DIRECTORY)
    db_session.commit()
    second = import_employee_source(db_session, SOURCE_DIRECTORY)
    assert first["status"] == "imported"
    assert second == {**first, "status": "reused"}
    assert db_session.scalar(select(func.count()).select_from(AssessmentEmployee)) == 290
    assert db_session.scalar(select(func.count()).select_from(AssessmentQuota)) == 14
    assert db_session.scalar(select(func.count()).select_from(AssessmentEmployee).where(
        AssessmentEmployee.is_salesperson.is_(True))) == 17


def test_import_rejects_modified_database_snapshot(db_session):
    import_employee_source(db_session, SOURCE_DIRECTORY)
    db_session.get(AssessmentEmployee, 275).job_title = "非法来源修改"
    db_session.flush()
    with pytest.raises(ValueError, match="快照"):
        import_employee_source(db_session, SOURCE_DIRECTORY)


def test_scope_permissions_and_role_changes(db_session, assessment_accounts):
    service = SalesMetricService()
    employee = principal_for(db_session, assessment_accounts["employee"].id)
    manager = principal_for(db_session, assessment_accounts["manager"].id)
    assert service.select_employees(employee, "self", []) == (275,)
    assert service.select_employees(manager, "team", []) == (275,)
    for principal, scope, ids in (
        (employee, "employees", [276]), (employee, "organization", []),
        (manager, "employees", [276]), (manager, "employees", [286]),
    ):
        with pytest.raises(AccessDenied):
            service.select_employees(principal, scope, ids)
    configure_access(db_session, user_id=assessment_accounts["employee"].id,
                     level="manager", employee_id=275, managed_employee_ids=[])
    db_session.flush()
    updated = principal_for(db_session, assessment_accounts["manager"].id)
    assert updated.allowed_employee_ids == (285,)


def test_account_binding_cannot_share_employee(db_session, assessment_accounts):
    with pytest.raises(ValueError, match="已经绑定"):
        configure_access(db_session, user_id=assessment_accounts["other"].id,
                         level="employee", employee_id=275, managed_employee_ids=[])
    with pytest.raises(ValueError, match="主管"):
        configure_access(db_session, user_id=assessment_accounts["manager"].id,
                         level="manager", employee_id=285, managed_employee_ids=[286])


def test_missing_orders_never_return_zero(db_session, assessment_accounts):
    service = SalesMetricService()
    employee = principal_for(db_session, assessment_accounts["employee"].id)
    with pytest.raises(DataBlocked, match="SalesOrderReadPort"):
        service.query(db_session, employee, scope="self", employee_ids=[])
    with pytest.raises(DataBlocked):
        service.evidence(db_session, employee, 275)


def test_historical_report_checks_current_team(db_session, assessment_accounts):
    run = AssessmentRun(owner_id=assessment_accounts["employee"].id, owner_level="employee",
                        scope_employee_ids=[275])
    manager = principal_for(db_session, assessment_accounts["manager"].id)
    assert_run_read(manager, run)
    other = principal_for(db_session, assessment_accounts["manager2"].id)
    with pytest.raises(AccessDenied):
        assert_run_read(other, run)
    configure_access(db_session, user_id=manager.user_id, level="manager",
                     employee_id=285, managed_employee_ids=[])
    with pytest.raises(AccessDenied):
        assert_run_read(principal_for(db_session, manager.user_id), run)

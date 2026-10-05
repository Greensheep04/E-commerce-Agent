import pytest
from pydantic import ValidationError

from shop.assessment_types import AssessmentQuery


@pytest.mark.parametrize("scope", ["self", "team", "organization"])
def test_invalid_scope_and_employee_combination_is_rejected_by_schema(scope):
    with pytest.raises(ValidationError, match="员工编号"):
        AssessmentQuery(action="metrics", scope=scope, employee_ids=[275])


def test_invalid_explicit_employee_scope_requires_identifiers():
    with pytest.raises(ValidationError, match="员工编号"):
        AssessmentQuery(action="metrics", scope="employees", employee_ids=[])

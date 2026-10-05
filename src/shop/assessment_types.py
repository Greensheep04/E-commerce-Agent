from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SOURCE_ID = "microsoft/adventureworks"
REVISION = "beaab06ef72831089ca80e5355d65e661fd19b26"
SOURCE_KEY = "adventureworks-q3-2023"
# 官方来源的日期字段没有时区。
PERIOD_START = datetime(2023, 7, 1)  # noqa: DTZ001
PERIOD_END = datetime(2023, 10, 1)  # noqa: DTZ001
RULE_VERSION = "sales-q3-v1"
SAMPLE_NOTICE = "Microsoft AdventureWorks 官方模拟公司样例；样例计算，币种未核实"
METRIC_IDS = ("sales_amount", "order_count", "average_order_value", "quota_completion")


class AssessmentQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    action: Literal["metrics", "insufficient", "clarify"]
    scope: Literal["self", "team", "employees", "organization"] = "self"
    employee_ids: list[int] = Field(default_factory=list, max_length=17)
    period_start: str = "2023-07-01"
    period_end: str = "2023-10-01"
    metric_ids: list[Literal["sales_amount", "order_count", "average_order_value", "quota_completion"]] = Field(
        default_factory=lambda: list(METRIC_IDS), min_length=1, max_length=4,
    )
    reason: str = Field(default="", max_length=1000)
    include_evidence: bool = False
    comparison_employee_ids: list[int] = Field(default_factory=list, max_length=17)

    @field_validator("metric_ids")
    @classmethod
    def unique_metric_ids(cls, value):
        if len(value) != len(set(value)):
            raise ValueError("指标编号不能重复")
        return value

    @model_validator(mode="after")
    def validate_employee_scope(self):
        if self.action == "metrics":
            if self.scope != "employees" and self.employee_ids:
                raise ValueError("self、team与organization范围的employee_ids必须为空，不同时指定员工编号")
            if self.scope == "employees" and (
                not self.employee_ids or len(set(self.employee_ids)) != len(self.employee_ids)
            ):
                raise ValueError("employees范围需要不重复的员工编号")
        return self


class AssessmentExplanation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    headline: str = Field(min_length=1, max_length=120)
    observations: list["Observation"] = Field(max_length=72)
    suggestions: list[str] = Field(default_factory=list, max_length=3)


class Observation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    employee_id: int | None
    metric_id: Literal["sales_amount", "order_count", "average_order_value", "quota_completion"]
    evidence_id: str
    text: str = Field(min_length=1, max_length=300)

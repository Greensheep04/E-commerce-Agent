from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

ALLOWED_ATTRIBUTES = {"gender", "masterCategory", "subCategory", "articleType",
                      "baseColour", "season", "usage"}


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RequirementPlan(StrictModel):
    count: int = Field(ge=1, le=2)
    attributes: dict[str, str] = Field(default_factory=dict)
    forbidden_attributes: dict[str, list[str]] = Field(default_factory=dict)
    must: list[str] = Field(default_factory=list, max_length=12)
    forbid: list[str] = Field(default_factory=list, max_length=12)
    queries: list[str] = Field(min_length=1, max_length=4)
    keywords: list[str] = Field(default_factory=list, max_length=8)
    required_facts: list[str] = Field(default_factory=list, max_length=10)
    page_requirements: str = ""

    @model_validator(mode="after")
    def valid_filters(self):
        if (set(self.attributes) | set(self.forbidden_attributes)) - ALLOWED_ATTRIBUTES:
            raise ValueError("筛选包含来源不支持的字段")
        if any(not item.strip() for item in self.queries):
            raise ValueError("查询文字为空")
        return self


class ImageDescription(StrictModel):
    description: str = Field(min_length=1, max_length=2000)
    visible_features: list[str] = Field(min_length=1, max_length=20)
    inferred_uses: list[str] = Field(default_factory=list, max_length=10)
    uncertainties: list[str] = Field(default_factory=list, max_length=10)


class Recommendation(StrictModel):
    asset_id: str = Field(min_length=1, max_length=36)
    caption: str = Field(min_length=1, max_length=1500)


class Draft(StrictModel):
    headline: str = Field(min_length=1, max_length=200)
    summary: str = Field(min_length=1, max_length=2000)
    recommendations: list[Recommendation] = Field(min_length=1, max_length=2)

    @model_validator(mode="after")
    def distinct_assets(self):
        ids = [item.asset_id for item in self.recommendations]
        if len(set(ids)) != len(ids):
            raise ValueError("推荐不能重复相同素材")
        return self


class Review(StrictModel):
    approved: bool
    issues: list[str] = Field(max_length=20)
    coverage: list[str] = Field(min_length=1, max_length=20)

    @model_validator(mode="after")
    def consistent_result(self):
        if self.approved == bool(self.issues):
            raise ValueError("审核结论与问题列表不一致")
        if any(not item.strip() for item in [*self.issues, *self.coverage]):
            raise ValueError("审核问题与检查依据不能包含空白内容")
        return self


class CampaignResult(StrictModel):
    status: Literal["materials_found", "insufficient"]
    headline: str = ""
    summary: str = ""
    recommendations: list[dict] = Field(default_factory=list, max_length=2)
    missing_info: list[str] = Field(default_factory=list)

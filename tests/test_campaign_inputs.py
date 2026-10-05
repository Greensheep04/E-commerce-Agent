import pytest
from pydantic import ValidationError


def test_requirement_plan_rejects_unavailable_filter_and_excess_count():
    from shop.campaign_types import RequirementPlan

    with pytest.raises(ValidationError):
        RequirementPlan.model_validate({"count": 2, "attributes": {"price": "10"},
                                        "queries": ["运动穿搭"]})
    with pytest.raises(ValidationError):
        RequirementPlan.model_validate({"count": 3, "attributes": {}, "queries": ["服装"]})


def test_review_rejection_requires_actionable_issue():
    from shop.campaign_types import Review

    with pytest.raises(ValidationError):
        Review.model_validate({"approved": False, "issues": []})


def test_review_rejects_saved_response_without_coverage():
    from shop.campaign_types import Review

    # 真实请求f34deac7-70a7-45ef-ba99-94de3b125200的审核正文。
    response = '{"approved": false, "issues": ["文案中声称“这款 Classic Polo 男士蓝色条纹T恤”，但来源素材中的 `productDisplayName` 为 "]}'
    with pytest.raises(ValidationError, match="coverage"):
        Review.model_validate_json(response)


@pytest.mark.parametrize("coverage", [[], [""], ["  "]])
def test_review_requires_nonempty_check_evidence(coverage):
    from shop.campaign_types import Review

    with pytest.raises(ValidationError):
        Review.model_validate({"approved": True, "issues": [], "coverage": coverage})


@pytest.mark.parametrize("issue", ["", "  "])
def test_review_rejects_empty_issue_text(issue):
    from shop.campaign_types import Review

    with pytest.raises(ValidationError):
        Review.model_validate({"approved": False, "issues": [issue],
                               "coverage": ["已检查数量、需求条件及商品来源。"]})


def test_draft_cannot_repeat_the_same_asset():
    from shop.campaign_types import Draft

    with pytest.raises(ValidationError):
        Draft.model_validate({"headline": "宣传", "summary": "介绍", "recommendations": [
            {"asset_id": "a", "caption": "介绍"}, {"asset_id": "a", "caption": "介绍"},
        ]})

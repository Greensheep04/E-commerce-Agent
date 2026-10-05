import json
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import httpx

from shop.config import Settings
from shop.fashion import EXPECTED_COUNT, REVISION, SOURCE_ID


def main():
    settings = Settings()
    run_id = f"m1-http-{datetime.now(UTC):%Y%m%dT%H%M%S}-{uuid4().hex[:8]}"
    evidence_dir = Path(".scratch/evidence") / run_id
    evidence_dir.mkdir(parents=True, exist_ok=False)
    checks = []
    with httpx.Client(base_url="http://127.0.0.1:8000", timeout=30) as client:
        health = client.get("/health")
        health.raise_for_status()
        assert health.json() == {"status": "ok"}
        assert client.get("/api/assets").status_code == 401
        incomplete_login = client.post("/admin/login", data={"username": "admin"})
        assert incomplete_login.status_code == 400
        assert client.get("/api/assets").status_code == 401
        checks.append("health_and_anonymous_access")
        password = settings.admin_password_file.read_text(encoding="utf-8").strip()
        login = client.post("/api/login", json={"username": "admin", "password": password})
        login.raise_for_status()
        assert login.json()["role"] == "admin"
        assets = []
        for offset in (0, 200):
            response = client.get("/api/assets", params={
                "source_id": SOURCE_ID, "source_revision": REVISION,
                "limit": 200, "offset": offset,
            })
            response.raise_for_status()
            assets.extend(response.json())
        assert len(assets) == EXPECTED_COUNT
        assert len({asset["id"] for asset in assets}) == EXPECTED_COUNT
        assert [asset["source_order"] for asset in assets] == list(range(EXPECTED_COUNT))
        checks.append("authenticated_pagination_and_source_identity")
        asset = assets[0]
        response = client.get(f"/api/assets/{asset['id']}")
        response.raise_for_status()
        detail = response.json()
        version = detail["versions"][-1]
        file_url = f"/api/assets/{asset['id']}/versions/{version['version']}/file"
        file_response = client.get(file_url)
        file_response.raise_for_status()
        assert sha256(file_response.content).hexdigest() == version["file_sha256"]
        assert file_response.headers["x-content-type-options"] == "nosniff"
        checks.append("authenticated_file_integrity")
        admin_page = client.get("/admin/asset/list")
        admin_page.raise_for_status()
        assert "/manage/materials/" in admin_page.text
        editor = client.get(f"/manage/materials/{asset['id']}/edit")
        editor.raise_for_status()
        assert file_url in editor.text
        checks.append("admin_page_and_version_editor")
        client.post("/api/logout").raise_for_status()
        assert client.get(file_url).status_code == 401
        checks.append("logout_revokes_file_access")
    report = {
        "run_id": run_id,
        "entrypoint": "http://127.0.0.1:8000",
        "source_id": SOURCE_ID,
        "revision": REVISION,
        "asset_count": len(assets),
        "checked_asset_id": asset["id"],
        "checked_file_sha256": version["file_sha256"],
        "checks": checks,
        "status": "passed",
    }
    report_path = evidence_dir / "report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({"status": "passed", "run_id": run_id, "report": str(report_path)}, ensure_ascii=False))


if __name__ == "__main__":
    main()

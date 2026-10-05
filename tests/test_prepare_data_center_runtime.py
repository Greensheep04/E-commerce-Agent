from stat import S_IMODE

import pytest

from scripts import prepare_data_center_runtime as runtime


def test_service_identity_generation_creates_private_file_and_preserves_existing_identity(tmp_path):
    path = tmp_path / "auth" / "data-center.service.key"
    first = runtime.prepare_service_identity(path)
    original = path.read_bytes()
    assert first == {"path": str(path.resolve()), "created": True}
    assert len(original.strip()) >= 64
    assert S_IMODE(path.stat().st_mode) == 0o600
    second = runtime.prepare_service_identity(path)
    assert second == {"path": str(path.resolve()), "created": False}
    assert path.read_bytes() == original


def test_service_identity_preparation_rejects_invalid_existing_content(tmp_path):
    path = tmp_path / "service.key"
    path.write_text("invalid", encoding="utf-8")
    path.chmod(0o600)
    with pytest.raises(ValueError, match="身份凭据"):
        runtime.prepare_service_identity(path)
    assert path.read_text(encoding="utf-8") == "invalid"


def test_service_identity_preparation_requires_private_permissions(tmp_path):
    path = tmp_path / "service.key"
    path.write_text("a" * 64, encoding="utf-8")
    path.chmod(0o644)
    with pytest.raises(PermissionError, match="0600"):
        runtime.prepare_service_identity(path)

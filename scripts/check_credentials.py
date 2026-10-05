import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
from dotenv import dotenv_values


def main():
    parser = argparse.ArgumentParser(description="只检查提供商配置是否包含非空凭据")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--probe-deepseek", action="store_true")
    arguments = parser.parse_args()
    if not arguments.env_file.is_file():
        raise FileNotFoundError("指定的提供商配置文件不存在")
    values = dotenv_values(arguments.env_file)
    result = {
        "source": str(arguments.env_file.resolve()),
        "credentials": {
            name: bool(values.get(name) and values[name].strip())
            for name in ("DEEPSEEK_API_KEY", "DASHSCOPE_API_KEY")
        },
        "network_verified": False,
    }
    if arguments.probe_deepseek:
        key = values["DEEPSEEK_API_KEY"]
        if not key or not key.strip():
            raise ValueError("DeepSeek 凭据为空")
        response = httpx.get(
            "https://api.deepseek.com/user/balance",
            headers={"Authorization": f"Bearer {key}"},
            timeout=20,
        )
        response.raise_for_status()
        result["deepseek_available"] = response.json()["is_available"]
        result["network_verified"] = True
        result["checked_at"] = datetime.now(UTC).isoformat()
        report_dir = Path(".scratch/evidence/provider-readiness")
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"deepseek-{datetime.now(UTC):%Y%m%dT%H%M%S}.json"
        with report_path.open("x", encoding="utf-8") as output:
            json.dump(result, output, ensure_ascii=False, indent=2)
        result["report"] = str(report_path)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

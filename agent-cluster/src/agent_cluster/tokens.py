import hashlib
import json
from functools import lru_cache
from pathlib import Path

from tokenizers import Tokenizer

from agent_cluster.types import CoordinatorProfile


@lru_cache(maxsize=8)
def _tokenizer(path: str, digest: str) -> Tokenizer:
    return Tokenizer.from_file(path)


def estimate_tokens(messages: list[dict], tools: object, profile: CoordinatorProfile | None = None) -> int:
    serialized = json.dumps({"messages": messages, "tools": tools}, ensure_ascii=False,
                            separators=(",", ":"), sort_keys=True, allow_nan=False)
    if profile is None or profile.tokenizer_file is None:
        return len(serialized.encode("utf-8"))
    path = Path(profile.tokenizer_file).resolve(strict=True)
    content_digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if content_digest != profile.tokenizer_sha256:
        raise ValueError("Tokenizer文件来源摘要不匹配")
    tokenizer = _tokenizer(str(path), content_digest)
    count = len(tokenizer.encode(serialized, add_special_tokens=False).ids)
    # 完整JSON保留reasoning和工具Schema；额外预留消息格式及提供商格式差异。
    return (count * 110 + 99) // 100 + 32 * len(messages) + 128

from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit

from integration_protocol import Identifier, ResourceRef
from pydantic import TypeAdapter, ValidationError

from agent_hub.core import HubError


def parse_resource_uri(uri: str) -> tuple[str, ResourceRef, str]:
    try:
        if any(ord(character) < 32 or ord(character) == 127 for character in uri):
            raise ValueError("资源地址包含控制字符")
        parsed = urlsplit(uri)
        if (parsed.scheme != "agenthub" or not parsed.netloc or parsed.username is not None
                or parsed.password is not None or parsed.port is not None or parsed.fragment):
            raise ValueError("资源地址格式无效")
        connector_id = TypeAdapter(Identifier).validate_python(parsed.netloc)
        if not parsed.path.startswith("/"):
            raise ValueError("资源地址缺少路径")
        parts = [unquote(part, errors="strict") for part in parsed.path[1:].split("/")]
        if any(ord(character) < 32 or ord(character) == 127 for part in parts for character in part):
            raise ValueError("资源路径包含控制字符")
        if len(parts) < 4:
            raise ValueError("资源地址缺少必要字段")
        relative_parts = parts[3:]
        if any(part in {"", ".", ".."} or "/" in part or "\\" in part or "\x00" in part
               for part in relative_parts):
            raise ValueError("文件路径格式无效")
        entries = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
        query = dict(entries)
        if len(query) != len(entries) or set(query) - {"version", "sha256"}:
            raise ValueError("资源查询参数无效或重复")
        reference = ResourceRef(module_id=parts[0], resource_type=parts[1], resource_id=parts[2], **query)
        return connector_id, reference, "/".join(relative_parts)
    except (ValueError, ValidationError, UnicodeError) as error:
        raise HubError("invalid_resource_uri", "资源地址、路径或引用参数无效") from error


def build_resource_uri(connector_id: str, reference: ResourceRef, relative_path: str) -> str:
    reference = ResourceRef.model_validate(reference)
    path = "/".join(quote(value, safe="") for value in (
        reference.module_id, reference.resource_type, reference.resource_id,
    ))
    uri = f"agenthub://{connector_id}/{path}/{quote(relative_path, safe='/')}"
    query = {key: getattr(reference, key) for key in ("version", "sha256")
             if getattr(reference, key) is not None}
    if query:
        uri += "?" + urlencode(query)
    parse_resource_uri(uri)
    return uri

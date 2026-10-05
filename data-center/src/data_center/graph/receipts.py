import os
from pathlib import Path
from typing import Literal
from uuid import uuid4

from data_center_protocol import ModelInvocation, ModelReply
from pydantic import BaseModel, ConfigDict

from data_center.documents.conversion import identity_hash
from data_center.documents.gateway import reply_vectors


class EmbeddingReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    graph_fingerprint: str
    actor_id: str
    invocation: ModelInvocation
    reply: ModelReply
    invocation_sha256: str
    response_sha256: str


class EmbeddingReceipts:
    def __init__(self, directory: Path, fingerprint: str, actor_id: str, source_ids: list[str]):
        self.directory = directory
        self.fingerprint = fingerprint
        self.actor_id = actor_id
        self.source_ids = sorted(source_ids)
        self.records = {}
        self.by_text = {}
        if directory.exists():
            for path in sorted(directory.glob("*.json")):
                if path.name.startswith("."):
                    continue
                receipt = EmbeddingReceipt.model_validate_json(path.read_bytes())
                self._validate(receipt)
                key = self._key(receipt.invocation)
                if path.name != key + ".json":
                    raise ValueError("图embedding receipt文件编号与请求内容不一致")
                self._remember(key, receipt)

    def _key(self, request):
        return identity_hash({"actor": self.actor_id, "graph": self.fingerprint,
                              "model": request.model, "step": request.step_key,
                              "sources": self.source_ids})

    def _validate(self, receipt):
        request, reply = receipt.invocation, receipt.reply
        if (receipt.graph_fingerprint != self.fingerprint or receipt.actor_id != self.actor_id
                or sorted(request.source_ids) != self.source_ids):
            raise PermissionError("图embedding receipt超出当前身份或来源范围")
        if request.kind != "embedding" or request.model != "qwen3.7-text-embedding":
            raise ValueError("图embedding receipt模型或请求种类无效")
        expected_step = "graph-embedding:" + identity_hash({"graph": self.fingerprint,
                                                           "texts": request.texts})
        if request.step_key != expected_step:
            raise ValueError("图embedding receipt步骤与完整输入不一致")
        if (receipt.invocation_sha256 != identity_hash(request.model_dump(mode="json"))
                or receipt.response_sha256 != identity_hash(reply.response)):
            raise ValueError("图embedding receipt请求或响应摘要不一致")
        if reply.status != "completed":
            raise ValueError("图embedding receipt没有completed真实响应")
        reply_vectors(reply, len(request.texts))

    def _remember(self, key, receipt):
        if key != self._key(receipt.invocation):
            raise ValueError("图embedding receipt文件编号与请求内容不一致")
        if key in self.records:
            raise ValueError("图embedding receipt批次编号重复")
        self.records[key] = receipt
        for text in receipt.invocation.texts:
            self.by_text.setdefault(text, key)

    def selected(self, texts):
        keys = list(dict.fromkeys(self.by_text[text] for text in texts if text in self.by_text))
        return [self.records[key] for key in keys]

    def save(self, request, reply):
        receipt = EmbeddingReceipt(graph_fingerprint=self.fingerprint, actor_id=self.actor_id,
            invocation=request, reply=reply,
            invocation_sha256=identity_hash(request.model_dump(mode="json")),
            response_sha256=identity_hash(reply.response))
        self._validate(receipt)
        key = self._key(request)
        if key in self.records:
            return self.records[key]
        self.directory.mkdir(parents=True, exist_ok=True)
        target = self.directory / (key + ".json")
        if target.exists():
            existing = EmbeddingReceipt.model_validate_json(target.read_bytes())
            self._validate(existing)
            self._remember(key, existing)
            return existing
        pending = self.directory / f".pending-{uuid4()}.json"
        # Graph数据库锁串行化写入；完整文件同步后以原子替换提交。
        with pending.open("xb") as stream:
            stream.write((receipt.model_dump_json(indent=2) + "\n").encode())
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(pending, target)
        descriptor = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self._remember(key, receipt)
        return receipt

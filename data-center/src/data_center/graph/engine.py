import asyncio
import os
from importlib.metadata import version
from typing import Literal

import numpy as np
from data_center_protocol import (
    AuthorizedScope,
    DataEvidenceRef,
    DocumentSearchHit,
    DocumentSearchResult,
    ModelInvocation,
)
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from data_center.database import authorized_connection
from data_center.documents.conversion import identity_hash
from data_center.documents.gateway import (
    ModelStepPending,
    embedding_batches,
    invoke_step,
    reply_content,
    reply_vectors,
    reuse_completed_step,
)
from data_center.documents.locking import database_lock
from data_center.documents.models import (
    DocumentSnapshotRow,
    GraphChunkRefRow,
    GraphIndexRow,
    GraphScopeAliasRow,
)
from data_center.graph.identity import graph_chunk_mapping, graph_fingerprint
from data_center.graph.receipts import EmbeddingReceipts
from data_center.store import DataTask, digest


class GraphConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    enabled: bool = False
    chat_model: Literal["qwen3.8-flash", "deepseek-flash"] = "qwen3.8-flash"
    retrieval_mode: str = "mix"
    top_k: int = Field(default=10, ge=1, le=100)
    max_total_tokens: int = Field(default=8192, ge=1024, le=32768)
    entity_extract_max_gleaning: int = Field(default=0, ge=0, le=2)
    cosine_threshold: float = Field(default=0.2, ge=-1, le=1)


class GraphEngine:
    def __init__(self, documents, config: GraphConfig | None):
        self.documents = documents
        self.database = documents.database
        self.config = config or GraphConfig()
        self.call_ids = []
        self.callback_error = None
        self.embedding_receipts = {}
        if self.config.retrieval_mode not in {"local", "global", "hybrid", "mix"}:
            raise ValueError("图检索模式无效")
        self.config_identity = {"config": self.config.model_dump(mode="json"),
                                "lightrag": version("lightrag-hku"),
                                "embedding": "qwen3.7-text-embedding", "dimensions": 1024,
                                "chat": self.config.chat_model, "tokenizer": "cl100k_base",
                                "graph_storage": "PGTableGraphStorage", "vector_storage": "PGVectorStorage"}
        if self.config_identity["lightrag"] != "1.5.7":
            raise ValueError("LightRAG版本与图索引要求不一致")

    def _identity(self, scope, snapshots, rows):
        keys = [f"{row.snapshot_id}:{row.chunk_id}" for row in rows]
        fingerprint = graph_fingerprint([item.snapshot_id for item in snapshots], keys,
                                        scope.policy_version,
                                        {**self.config_identity, "scope_fingerprint": scope.fingerprint})
        with authorized_connection(self.database, scope) as connection, Session(connection) as session:
            if session.get(GraphIndexRow, fingerprint) is not None:
                return fingerprint
            aliases = session.scalars(select(GraphIndexRow).join(
                GraphScopeAliasRow, GraphScopeAliasRow.graph_fingerprint == GraphIndexRow.fingerprint
            ).where(GraphScopeAliasRow.actor_id == scope.actor_id,
                    GraphScopeAliasRow.policy_version == scope.policy_version,
                    GraphScopeAliasRow.scope_fingerprint == scope.fingerprint)
                .order_by(GraphIndexRow.fingerprint)).all()
            for index in aliases:
                if self._matching_index(index, snapshots, keys):
                    return index.fingerprint
        return fingerprint

    def _matching_index(self, index, snapshots, keys):
        return (index.snapshot_ids == sorted(item.snapshot_id for item in snapshots)
                and index.allowed_chunk_ids == sorted(keys)
                and index.chunk_count == len(keys)
                and index.config_sha256 == identity_hash(self.config_identity))

    async def resolve_identity(self, query, scope, snapshots, rows, context):
        fingerprint = self._identity(scope, snapshots, rows)
        with authorized_connection(self.database, scope) as connection, Session(connection) as session:
            if session.get(GraphIndexRow, fingerprint) is not None:
                return fingerprint
        keys = [f"{row.snapshot_id}:{row.chunk_id}" for row in rows]
        if len(set(keys)) != len(keys):
            raise ValueError("图资料片段集合包含重复编号")
        with Session(self.database) as session:
            candidates = session.scalars(select(GraphIndexRow).where(
                GraphIndexRow.config_sha256 == identity_hash(self.config_identity)
            ).order_by(GraphIndexRow.fingerprint)).all()
            candidates = [index for index in candidates if self._matching_index(index, snapshots, keys)]
            for index in candidates:
                session.expunge(index)
        current_values = scope.model_dump(mode="json", exclude={"policy_version", "fingerprint"})
        for index in candidates:
            with Session(self.database) as session:
                tasks = session.scalars(select(DataTask).where(
                    DataTask.actor_id == scope.actor_id,
                    DataTask.operation.in_(["build_graph", "query", "ask"]),
                    DataTask.checkpoints["authorization"]["policy_version"].astext == index.policy_version,
                    DataTask.checkpoints["authorization"]["fingerprint"].astext == index.scope_fingerprint,
                ).order_by(DataTask.created_at, DataTask.task_id)).all()
                for task in tasks:
                    session.expunge(task)
            for task in tasks:
                source_ids = task.payload["source_ids"]
                if len(set(source_ids)) != len(source_ids) or set(source_ids) != set(query.source_ids):
                    continue
                if digest({"payload": task.payload, "upstream": task.upstream}) != task.input_sha256:
                    raise ValueError("历史图任务输入摘要不一致")
                original = AuthorizedScope.model_validate(task.checkpoints["authorization"])
                if original.model_dump(mode="json", exclude={"policy_version", "fingerprint"}) != current_values:
                    continue
                expected = graph_fingerprint([item.snapshot_id for item in snapshots], keys,
                    original.policy_version, {**self.config_identity, "scope_fingerprint": original.fingerprint})
                if expected != index.fingerprint:
                    continue
                historical_order = await self.documents._scope(context, source_ids, "read")
                if historical_order != original:
                    continue
                await self.documents._scope(context, query.source_ids, "read", scope)
                alias_id = identity_hash({"actor": scope.actor_id, "policy": scope.policy_version,
                                          "scope": scope.fingerprint, "graph": index.fingerprint})
                proof = {"schema_version": 1, "task_id": task.task_id,
                         "input_sha256": task.input_sha256, "source_ids": source_ids,
                         "snapshot_ids": index.snapshot_ids, "allowed_chunk_ids": index.allowed_chunk_ids,
                         "config_sha256": index.config_sha256,
                         "original_authorization": original.model_dump(mode="json"),
                         "historical_order_authorization": historical_order.model_dump(mode="json"),
                         "current_authorization": scope.model_dump(mode="json")}
                async with database_lock(self.database, f"dc-graph-alias:{alias_id}"):
                    await self.documents._scope(context, query.source_ids, "read", scope)
                    with Session(self.database) as session, session.begin():
                        alias = session.get(GraphScopeAliasRow, alias_id)
                        if alias is None:
                            session.add(GraphScopeAliasRow(alias_id=alias_id, actor_id=scope.actor_id,
                                policy_version=scope.policy_version, scope_fingerprint=scope.fingerprint,
                                graph_fingerprint=index.fingerprint, proof=proof))
                        elif (alias.actor_id != scope.actor_id or alias.policy_version != scope.policy_version
                              or alias.scope_fingerprint != scope.fingerprint
                              or alias.graph_fingerprint != index.fingerprint):
                            raise ValueError("图权限身份关联内容冲突")
                return index.fingerprint
        return fingerprint

    def status(self, scope, snapshots, rows):
        fingerprint = self._identity(scope, snapshots, rows)
        with authorized_connection(self.database, scope) as connection, Session(connection) as session:
            row = session.get(GraphIndexRow, fingerprint)
            if row is None:
                return {"fingerprint": fingerprint, "state": "absent", "chunk_count": len(rows),
                        "enabled": self.config.enabled}
            return {"fingerprint": fingerprint, "state": row.state, "chunk_count": row.chunk_count,
                    "call_ids": row.call_ids, "enabled": self.config.enabled}

    def _validate_database(self):
        from lightrag.kg.postgres_impl import ClientManager

        configured = ClientManager.get_config(vector_storage="PGVectorStorage")
        url = self.database.url
        expected = (url.host, int(url.port or 5432), url.database, url.username)
        actual = (configured["host"], int(configured["port"]), configured["database"], configured["user"])
        if expected != actual or (configured["password"] or "") != (url.password or ""):
            raise ValueError("LightRAG PostgreSQL配置与宿主writer数据库不一致")
        if not os.environ.get("TIKTOKEN_CACHE_DIR"):
            raise ValueError("缺少项目 tokenizer 缓存目录")

    def _save_call(self, fingerprint, call_id):
        if call_id not in self.call_ids:
            self.call_ids.append(call_id)
        with Session(self.database) as session, session.begin():
            row = session.get(GraphIndexRow, fingerprint)
            if row is not None:
                row.call_ids = list(dict.fromkeys([*row.call_ids, call_id]))

    def _callback_active(self):
        if self.callback_error is not None:
            raise self.callback_error

    def _capture_callback_error(self, error):
        if self.callback_error is None:
            self.callback_error = error

    async def _sdk_result(self, operation):
        try:
            result = await operation
        except BaseException:
            self._callback_active()
            raise
        self._callback_active()
        return result

    async def _embedding(self, fingerprint, texts, query, scope, snapshots, context, task_id, run_id):
        sources = query.source_ids
        cache_key = (fingerprint, context.actor_id, tuple(sorted(sources)))
        if cache_key not in self.embedding_receipts:
            self.embedding_receipts[cache_key] = EmbeddingReceipts(
                self.documents.artifact_root / "graph" / fingerprint / "embedding-results",
                fingerprint, context.actor_id, sources)
        receipts = self.embedding_receipts[cache_key]
        values = []
        for selected in embedding_batches(texts):
            self._callback_active()
            await self.documents._scope(context, sources, "model_dispatch", scope)
            self.documents._assert_published(scope, snapshots)
            key = "graph-embedding:" + identity_hash({"graph": fingerprint, "texts": selected})
            request = ModelInvocation(task_id=task_id, run_id=run_id, step_key=key,
                kind="embedding", model="qwen3.7-text-embedding", texts=selected, source_ids=sources)
            current = await self.documents.models.lookup(task_id, key, context)
            if current is not None and current.status == "completed":
                vectors = reply_vectors(current, len(selected))
                self._save_call(fingerprint, current.request_id)
                receipts.save(request, current)
                values.extend(vectors)
                await self.documents._scope(context, sources, "model_dispatch", scope)
                continue
            cached = receipts.selected(selected)
            if current is not None and (current.status != "reserved" or cached):
                raise ModelStepPending(current)
            restored = {}
            for receipt in cached:
                original = receipt.invocation.model_copy(update={"task_id": task_id, "run_id": run_id,
                                                                 "source_ids": sources})
                await self.documents._scope(context, sources, "model_dispatch", scope)
                self.documents._assert_published(scope, snapshots)
                reply = await reuse_completed_step(self.documents.models, original, receipt.reply, context,
                                                    before_reuse=self._callback_active)
                self._save_call(fingerprint, reply.request_id)
                vectors = reply_vectors(reply, len(original.texts))
                restored.update(zip(original.texts, vectors))
                await self.documents._scope(context, sources, "model_dispatch", scope)
            missing = [item for item in selected if item not in restored]
            if missing:
                missing_key = "graph-embedding:" + identity_hash({"graph": fingerprint, "texts": missing})
                missing_request = request.model_copy(update={"step_key": missing_key, "texts": missing})
                reply = await invoke_step(self.documents.models, missing_request, context,
                                          before_invoke=self._callback_active)
                self._save_call(fingerprint, reply.request_id)
                vectors = reply_vectors(reply, len(missing))
                receipts.save(missing_request, reply)
                restored.update(zip(missing, vectors))
                await self.documents._scope(context, sources, "model_dispatch", scope)
            values.extend(restored[item] for item in selected)
        return np.asarray(values, dtype=np.float32)

    async def _rag(self, fingerprint, query, scope, context, task_id, run_id):
        from lightrag import LightRAG
        from lightrag.utils import EmbeddingFunc, TiktokenTokenizer

        self._validate_database()
        sources = query.source_ids
        with authorized_connection(self.database, scope) as connection, Session(connection) as session:
            snapshots = list(session.get(GraphIndexRow, fingerprint).snapshot_ids)

        async def embedding(texts, **kwargs):
            try:
                return await self._embedding(fingerprint, texts, query, scope, snapshots,
                                             context, task_id, run_id)
            except BaseException as error:
                self._capture_callback_error(error)
                if isinstance(error, ModelStepPending):
                    self._save_call(fingerprint, error.reply.request_id)
                raise

        async def chat(prompt, system_prompt=None, history_messages=None, **kwargs):
            self._callback_active()
            if kwargs.get("stream"):
                raise ValueError("结构化图检索不接受stream模型响应")
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.extend(history_messages or [])
            messages.append({"role": "user", "content": prompt})
            try:
                await self.documents._scope(context, sources, "model_dispatch", scope)
                self.documents._assert_published(scope, snapshots)
                key = "graph-chat:" + identity_hash({"graph": fingerprint, "messages": messages,
                                                      "model": self.config.chat_model})
                request = ModelInvocation(task_id=task_id, run_id=run_id, step_key=key,
                                          kind="chat", model=self.config.chat_model, messages=messages,
                                          source_ids=sources, max_output_tokens=8192)
                reply = await invoke_step(self.documents.models, request, context,
                                          before_invoke=self._callback_active)
                self._save_call(fingerprint, reply.request_id)
                content = reply_content(reply)
                await self.documents._scope(context, sources, "model_dispatch", scope)
                return content
            except BaseException as error:
                self._capture_callback_error(error)
                if isinstance(error, ModelStepPending):
                    self._save_call(fingerprint, error.reply.request_id)
                raise

        directory = self.documents.artifact_root / "graph" / fingerprint
        directory.mkdir(parents=True, exist_ok=True)
        rag = LightRAG(working_dir=str(directory), workspace="dc-" + fingerprint,
                       kv_storage="PGKVStorage", graph_storage="PGTableGraphStorage",
                       vector_storage="PGVectorStorage", doc_status_storage="PGDocStatusStorage",
                       llm_model_func=chat, llm_model_name=self.config.chat_model, llm_model_max_async=1,
                       embedding_func=EmbeddingFunc(embedding_dim=1024, func=embedding,
                                                     model_name="qwen3.7-text-embedding", max_token_size=8192),
                       embedding_batch_num=20, embedding_func_max_async=1,
                       tokenizer=TiktokenTokenizer("gpt-3.5-turbo"),
                       entity_extract_max_gleaning=self.config.entity_extract_max_gleaning,
                       cosine_better_than_threshold=self.config.cosine_threshold,
                       enable_llm_cache=True, enable_llm_cache_for_entity_extract=True)
        try:
            await rag.initialize_storages()
        except BaseException as error:
            try:
                await rag.finalize_storages()
            except BaseException as cleanup_error:
                raise error from cleanup_error
            raise
        return rag

    def _state(self, fingerprint, state):
        with Session(self.database) as session, session.begin():
            session.get(GraphIndexRow, fingerprint).state = state

    async def _finalize(self, rag, fingerprint):
        try:
            await rag.finalize_storages()
        except BaseException as error:
            if self.callback_error is not None:
                self._callback_active()
            self._state(fingerprint, "cancelled" if isinstance(error, asyncio.CancelledError) else "failed")
            raise

    async def _eligible(self, scope, snapshots, rows):
        if not self.config.enabled or not rows:
            return False
        with authorized_connection(self.database, scope) as connection, Session(connection) as session:
            flags = [session.get(DocumentSnapshotRow, item.snapshot_id).graph_enabled for item in snapshots]
        return all(flags)

    @staticmethod
    def _vector_payloads(nodes, edges, mapping, global_config):
        from lightrag.constants import GRAPH_FIELD_SEP
        from lightrag.operate import _truncate_vdb_content
        from lightrag.utils import compute_mdhash_id

        def sources(item):
            values = item["source_id"].split(GRAPH_FIELD_SEP)
            if not values or any(not value or value not in mapping for value in values):
                raise ValueError("图实体或关系缺少当前授权原文映射")

        entities, relationships = {}, {}
        names = {node["id"] for node in nodes}
        if len(names) != len(nodes):
            raise ValueError("图实体编号重复")
        for node in nodes:
            sources(node)
            name = node["id"]
            if not name or node["entity_id"] != name:
                raise ValueError("图实体编号与属性不一致")
            key = compute_mdhash_id(name, prefix="ent-")
            if key in entities:
                raise ValueError("图实体向量编号重复")
            entities[key] = {"entity_name": name, "entity_type": node["entity_type"],
                             "content": _truncate_vdb_content(f"{name}\n{node['description']}",
                                                               global_config, f"entity:{name}"),
                             "source_id": node["source_id"], "file_path": node["file_path"]}
        for edge in edges:
            sources(edge)
            source, target = sorted((edge["source"], edge["target"]))
            if source not in names or target not in names:
                raise ValueError("图关系端点缺少实体")
            key = compute_mdhash_id(source + target, prefix="rel-")
            if key in relationships:
                raise ValueError("图关系向量编号重复")
            content = f"{edge['keywords']}\t{source}\n{target}\n{edge['description']}"
            relationships[key] = {"src_id": source, "tgt_id": target,
                                  "source_id": edge["source_id"],
                                  "content": _truncate_vdb_content(content, global_config,
                                                                    f"relationship:{source}-{target}"),
                                  "keywords": edge["keywords"], "description": edge["description"],
                                  "weight": edge["weight"], "file_path": edge["file_path"]}
        return entities, relationships

    def _missing_vectors(self, storage, expected, relationships=False):
        from lightrag.constants import GRAPH_FIELD_SEP

        table = self.database.dialect.identifier_preparer.quote(storage.table_name.lower())
        columns = "source_id, target_id" if relationships else "entity_name"
        with self.database.connect() as connection:
            records = connection.execute(text(
                f"SELECT id, {columns}, content, chunk_ids, file_path, "
                f"content_vector IS NOT NULL AS vector_present, vector_dims(content_vector) AS dimensions "
                f"FROM {table} WHERE workspace=:workspace"
            ), {"workspace": storage.workspace}).mappings().all()
        present = set()
        for record in records:
            key = record["id"]
            if key not in expected or key in present:
                raise ValueError("持久图向量编号与图记录不一致")
            payload = expected[key]
            identity_valid = ((record["source_id"], record["target_id"]) ==
                              (payload["src_id"], payload["tgt_id"])) if relationships else (
                                  record["entity_name"] == payload["entity_name"])
            if not identity_valid:
                raise ValueError("持久图向量实体或端点不一致")
            if (record["chunk_ids"] != payload["source_id"].split(GRAPH_FIELD_SEP)
                    or record["content"] != payload["content"]
                    or record["file_path"] != payload["file_path"]):
                raise ValueError("持久图向量内容或来源与图记录不一致")
            if not record["vector_present"] or record["dimensions"] != 1024:
                raise ValueError("持久图向量缺少有效1024维embedding")
            present.add(key)
        return {key: payload for key, payload in expected.items() if key not in present}

    async def _ensure_persistent_vectors(self, rag, fingerprint, scope, rows, mapping,
                                         query, context):
        indexed = {(row.snapshot_id, row.chunk_id): row for row in rows}
        with authorized_connection(self.database, scope) as connection, Session(connection) as session:
            references = list(session.scalars(select(GraphChunkRefRow).where(
                GraphChunkRefRow.fingerprint == fingerprint)))
            actual = {}
            for ref in references:
                key = (ref.snapshot_id, ref.chunk_id)
                original = indexed.get(key)
                if original is None or any(getattr(original, name) != getattr(ref, name)
                                           for name in ("source_id", "record_id", "asset_id", "text_sha256")):
                    raise ValueError("图持久引用与当前授权原文不一致")
                actual.setdefault(ref.graph_chunk_id, []).append(key)
        if {key: sorted(value) for key, value in actual.items()} != {
                key: sorted(value) for key, value in mapping.items()}:
            raise ValueError("图持久引用与当前片段编号不一致")
        nodes = await self._sdk_result(rag.chunk_entity_relation_graph.get_all_nodes())
        edges = await self._sdk_result(rag.chunk_entity_relation_graph.get_all_edges())
        entities, relationships = self._vector_payloads(nodes, edges, mapping, rag._build_global_config())
        # 全部来源和已有向量通过核查后，才能恢复缺失向量。
        missing_entities = self._missing_vectors(rag.entities_vdb, entities)
        missing_relationships = self._missing_vectors(rag.relationships_vdb, relationships, True)
        for storage, missing in ((rag.entities_vdb, missing_entities),
                                 (rag.relationships_vdb, missing_relationships)):
            if missing:
                await self.documents._scope(context, query.source_ids, "model_dispatch", scope)
                await self._sdk_result(storage.upsert(missing))
                await self._sdk_result(storage.index_done_callback())
        if (self._missing_vectors(rag.entities_vdb, entities)
                or self._missing_vectors(rag.relationships_vdb, relationships, True)):
            raise ValueError("图向量恢复后仍缺少持久记录")
        await self.documents._scope(context, query.source_ids, "read", scope)

    async def _insertion_rows(self, fingerprint, query, scope, snapshots, rows, context):
        alias_id = identity_hash({"actor": scope.actor_id, "policy": scope.policy_version,
                                  "scope": scope.fingerprint, "graph": fingerprint})
        with authorized_connection(self.database, scope) as connection, Session(connection) as session:
            alias = session.get(GraphScopeAliasRow, alias_id)
            if alias is None:
                return rows
            proof = alias.proof
            index = session.get(GraphIndexRow, fingerprint)
            keys = [f"{row.snapshot_id}:{row.chunk_id}" for row in rows]
            if (index is None or not self._matching_index(index, snapshots, keys)
                    or proof["schema_version"] != 1
                    or proof["config_sha256"] != index.config_sha256
                    or proof["snapshot_ids"] != index.snapshot_ids
                    or proof["allowed_chunk_ids"] != index.allowed_chunk_ids
                    or proof["current_authorization"] != scope.model_dump(mode="json")):
                raise ValueError("图输入排列缺少匹配的权限身份关联依据")
            snapshot_ids = list(index.snapshot_ids)
        source_ids = proof["source_ids"]
        if len(set(source_ids)) != len(source_ids) or set(source_ids) != set(query.source_ids):
            raise PermissionError("图输入原来源排列超出当前查询范围")
        original = AuthorizedScope.model_validate(proof["original_authorization"])
        if original != AuthorizedScope.model_validate(proof["historical_order_authorization"]):
            raise ValueError("图输入原来源排列的历史授权不一致")
        # 使用原来源顺序读取同一片段集合，保留LightRAG操作的完整输入排列。
        restored_scope, restored_snapshots, restored = await self.documents._visible(
            query.model_copy(update={"source_ids": list(source_ids)}), context, snapshot_ids)
        if restored_scope != original:
            raise PermissionError("图输入原来源排列的当前授权已经改变")

        def entries(values):
            return {(row.snapshot_id, row.chunk_id):
                    (row.source_id, row.record_id, row.asset_id, row.text_sha256, row.text, row.evidence)
                    for row in values}

        current_entries, restored_entries = entries(rows), entries(restored)
        if (len(current_entries) != len(rows) or len(restored_entries) != len(restored)
                or restored_entries != current_entries
                or sorted(item.snapshot_id for item in restored_snapshots) != snapshot_ids):
            raise ValueError("图输入原排列与当前授权资料片段不一致")
        await self.documents._scope(context, query.source_ids, "read", scope)
        return restored

    async def build(self, query, scope, snapshots, rows, context, task_id, run_id):
        if not await self._eligible(scope, snapshots, rows):
            return {"state": "DATA_BLOCKED", "reason": "当前配置或资料快照未启用图索引"}
        fingerprint = await self.resolve_identity(query, scope, snapshots, rows, context)
        async with database_lock(self.database, f"dc-graph:{fingerprint}"):
            await self.documents._scope(context, query.source_ids, "read", scope)
            with Session(self.database) as session, session.begin():
                index = session.get(GraphIndexRow, fingerprint)
                if index is None:
                    index = GraphIndexRow(fingerprint=fingerprint, scope_fingerprint=scope.fingerprint,
                                          policy_version=scope.policy_version, state="staged",
                                          snapshot_ids=sorted(item.snapshot_id for item in snapshots),
                                          allowed_chunk_ids=sorted(f"{row.snapshot_id}:{row.chunk_id}" for row in rows),
                                          config_sha256=identity_hash(self.config_identity), call_ids=[],
                                          chunk_count=len(rows))
                    session.add(index)
                published = index.state == "published"
                self.call_ids = list(index.call_ids)
            rows = await self._insertion_rows(fingerprint, query, scope, snapshots, rows, context)
            rag = None
            try:
                for row in rows:
                    await self.documents.read_evidence(DataEvidenceRef.model_validate(row.evidence), context)
                doc_id = "doc-" + fingerprint
                mapping = graph_chunk_mapping(doc_id, [(row.snapshot_id, row.chunk_id, row.text) for row in rows])
                rag = await self._rag(fingerprint, query, scope, context, task_id, run_id)
                previous_status = await rag.doc_status.get_by_id(doc_id)
                if previous_status is None or previous_status["status"] != "processed":
                    with Session(self.database) as session, session.begin():
                        for graph_id, references in mapping.items():
                            for snapshot_id, chunk_id in references:
                                key = (fingerprint, graph_id, snapshot_id, chunk_id)
                                if session.get(GraphChunkRefRow, key) is None:
                                    original = next(row for row in rows if row.snapshot_id == snapshot_id and row.chunk_id == chunk_id)
                                    session.add(GraphChunkRefRow(fingerprint=fingerprint, graph_chunk_id=graph_id,
                                                                 snapshot_id=snapshot_id, chunk_id=chunk_id,
                                                                 source_id=original.source_id, record_id=original.record_id,
                                                                 asset_id=original.asset_id, text_sha256=original.text_sha256))
                if not published:
                    await self._sdk_result(rag.ainsert_custom_chunks("".join(row.text for row in rows),
                                                                     [row.text for row in rows], doc_id=doc_id))
                status = await rag.doc_status.get_by_id(doc_id)
                if status is None or status["status"] != "processed":
                    raise ValueError("LightRAG图发布缺少processed状态")
                await self._ensure_persistent_vectors(rag, fingerprint, scope, rows, mapping, query, context)
                await self.documents._scope(context, query.source_ids, "read", scope)
                with Session(self.database) as session, session.begin():
                    index = session.get(GraphIndexRow, fingerprint)
                    index.state, index.call_ids = "published", list(self.call_ids)
                return {"state": "published", "fingerprint": fingerprint,
                        "chunk_count": len(rows), "call_ids": self.call_ids}
            except ModelStepPending as pending:
                status = pending.reply.status if pending.reply.status in {"unknown", "failed", "cancelled"} else "waiting"
                self._state(fingerprint, status)
                return {"state": status, "fingerprint": fingerprint, "call_ids": self.call_ids}
            except BaseException as error:
                self._capture_callback_error(error)
                self._state(fingerprint, "cancelled" if isinstance(error, asyncio.CancelledError) else "failed")
                raise
            finally:
                if rag is not None:
                    await self._finalize(rag, fingerprint)

    async def search(self, query, scope, snapshots, rows, context, task_id, run_id):
        built = await self.build(query, scope, snapshots, rows, context, task_id, run_id)
        if built["state"] != "published":
            return DocumentSearchResult(status=built["state"], call_ids=built.get("call_ids", []), metadata=built)
        fingerprint = built["fingerprint"]
        async with database_lock(self.database, f"dc-graph:{fingerprint}"):
            self.call_ids = list(built["call_ids"])
            from lightrag import QueryParam

            rag = None
            try:
                rag = await self._rag(fingerprint, query, scope, context, task_id, run_id)
                await self.documents._scope(context, query.source_ids, "read", scope)
                result = await self._sdk_result(rag.aquery_data(query.question, QueryParam(
                    mode=self.config.retrieval_mode, top_k=self.config.top_k,
                    chunk_top_k=query.limit, max_total_tokens=self.config.max_total_tokens,
                    enable_rerank=False)))
                await self.documents._scope(context, query.source_ids, "read", scope)
                if result["status"] != "success":
                    return DocumentSearchResult(status="DATA_BLOCKED", call_ids=self.call_ids,
                                                metadata={"reason": result.get("message", "图检索缺少结果")})
                return await self._map_result(result, fingerprint, query, scope, snapshots, rows, context)
            except ModelStepPending as pending:
                status = pending.reply.status if pending.reply.status in {"unknown", "failed", "cancelled"} else "waiting"
                return DocumentSearchResult(status=status, snapshots=snapshots, call_ids=self.call_ids)
            except BaseException as error:
                self._capture_callback_error(error)
                self._state(fingerprint, "cancelled" if isinstance(error, asyncio.CancelledError) else "failed")
                raise
            finally:
                if rag is not None:
                    await self._finalize(rag, fingerprint)

    async def validate_cached(self, result, rows, context):
        from lightrag.constants import GRAPH_FIELD_SEP

        fingerprint = result.metadata["fingerprint"]
        indexed = {(row.snapshot_id, row.chunk_id): row for row in rows}
        with Session(self.database) as session:
            index = session.get(GraphIndexRow, fingerprint)
            if index is None or index.state != "published":
                raise PermissionError("原图索引已撤回或未完成")
            allowed = {f"{snapshot_id}:{chunk_id}" for snapshot_id, chunk_id in indexed}
            if not set(index.allowed_chunk_ids).issubset(allowed):
                raise PermissionError("原图资料超出当前允许范围")
            references = list(session.scalars(select(GraphChunkRefRow).where(GraphChunkRefRow.fingerprint == fingerprint)))
            mapping = {}
            for ref in references:
                key = (ref.snapshot_id, ref.chunk_id)
                if key not in indexed or indexed[key].text_sha256 != ref.text_sha256:
                    raise PermissionError("原图引用超出当前允许片段集合")
                mapping.setdefault(ref.graph_chunk_id, []).append(key)
        graph = result.metadata["graph"]
        evidence = {}
        for category in ("entities", "relationships"):
            for position, item in enumerate(graph[category]):
                source_ids = item["source_id"].split(GRAPH_FIELD_SEP)
                if not source_ids or any(value not in mapping for value in source_ids):
                    raise ValueError("原图关系缺少有效原文映射")
                evidence[f"{category}:{position}"] = [list(key) for value in source_ids for key in mapping[value]]
        current_evidence = {key: sorted(values) for key, values in evidence.items()}
        cached_evidence = {key: sorted(values) for key, values in result.metadata["graph_evidence"].items()}
        if current_evidence != cached_evidence:
            raise ValueError("原图关系引用与持久映射不一致")
        for item in graph["chunks"]:
            graph_id = item["chunk_id"]
            if graph_id not in mapping or any(indexed[key].text.strip() != item["content"].strip()
                                              for key in mapping[graph_id]):
                raise ValueError("原图片段与不可变原文不一致")
        for key in {key for values in mapping.values() for key in values}:
            await self.documents.read_evidence(DataEvidenceRef.model_validate(indexed[key].evidence), context)

    async def _map_result(self, result, fingerprint, query, scope, snapshots, rows, context):
        from lightrag.constants import GRAPH_FIELD_SEP

        indexed = {(row.snapshot_id, row.chunk_id): row for row in rows}
        with authorized_connection(self.database, scope) as connection, Session(connection) as session:
            references = list(session.scalars(select(GraphChunkRefRow).where(GraphChunkRefRow.fingerprint == fingerprint)))
            mapping = {}
            for ref in references:
                key = (ref.snapshot_id, ref.chunk_id)
                if key not in indexed or indexed[key].text_sha256 != ref.text_sha256:
                    raise ValueError("图引用超出实际允许片段集合")
                mapping.setdefault(ref.graph_chunk_id, []).append(key)
        selected = []
        for item in result["data"]["chunks"]:
            graph_id = item["chunk_id"]
            if graph_id not in mapping:
                raise ValueError("LightRAG片段编号缺少真实原文映射")
            normalized = item["content"].strip()
            for key in mapping[graph_id]:
                if indexed[key].text.strip() != normalized:
                    raise ValueError("LightRAG片段内容与原文映射不一致")
                if key not in selected:
                    selected.append(key)
        graph_evidence = {}
        for category in ("entities", "relationships"):
            for index, item in enumerate(result["data"][category]):
                source_ids = item["source_id"].split(GRAPH_FIELD_SEP)
                if not source_ids or any(value not in mapping for value in source_ids):
                    raise ValueError("图关系缺少获准原文引用")
                graph_evidence[f"{category}:{index}"] = [list(key) for value in source_ids for key in mapping[value]]
        if not selected:
            return DocumentSearchResult(status="DATA_BLOCKED", call_ids=self.call_ids,
                                        metadata={"reason": "图结果没有可信原文片段"})
        hits = []
        for rank, key in enumerate(selected[:query.limit], 1):
            chunk = await self.documents.read_evidence(DataEvidenceRef.model_validate(indexed[key].evidence), context)
            hits.append(DocumentSearchHit(rank=rank, chunk=chunk))
        await self.documents._scope(context, query.source_ids, "read", scope)
        return DocumentSearchResult(status="completed", snapshots=snapshots, hits=hits, call_ids=self.call_ids,
                                    metadata={"retrieval_mode": "graph", "fingerprint": fingerprint,
                                              "graph": result["data"], "graph_evidence": graph_evidence,
                                              "processing": result.get("metadata", {})})

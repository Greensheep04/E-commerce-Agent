import argparse
import asyncio
import hashlib
import json
import os
import signal
from pathlib import Path

from data_center_protocol import DataQuery
from sqlalchemy import create_engine

from data_center.documents.engine import DocumentEngine
from data_center.graph.engine import GraphConfig, GraphEngine
from data_center.ports import ShopPorts


class NoPaidPorts(ShopPorts):
    async def invoke(self, request, context):
        raise PermissionError("当前真实恢复检查禁止新增付费调用")


async def main(arguments):
    definition = json.loads(Path(arguments.input).read_text())
    spec = json.loads(Path(definition["spec_path"]).read_text())
    actor = json.loads(Path(spec["identities_path"]).read_text())["actors"]["admin"]
    database = create_engine(spec["database_url"])
    ports = NoPaidPorts(spec["host_url"])
    try:
        context = await ports.login(actor["username"], Path(actor["password_file"]).read_text().strip(),
                                    definition["request_id"])
        documents = DocumentEngine(database, Path(spec["artifact_root"]), ports, ports, ports,
                                   graph_config=GraphConfig(enabled=True))
        query = DataQuery.model_validate(definition["query"])
        scope, snapshots, rows = await documents._visible(query, context)
        graph = GraphEngine(documents, documents.graph_config)
        fingerprint = await graph.resolve_identity(query, scope, snapshots, rows, context)
        assert fingerprint == definition["fingerprint"]
        identity = definition["seed"] if arguments.mode == "seed" else definition["restore"]
        texts = definition["texts"] if arguments.mode == "seed" else definition["reordered_texts"]
        values = await graph._embedding(fingerprint, texts, query, scope,
            [item.snapshot_id for item in snapshots], context, identity["task_id"], identity["run_id"])
        target = Path(definition[arguments.mode + "_output"])
        target.write_text(json.dumps({"pid": os.getpid(), "fingerprint": fingerprint,
            "shape": list(values.shape), "values_sha256": hashlib.sha256(values.tobytes()).hexdigest(),
            "call_ids": graph.call_ids,
            "usage": await ports.usage(identity["task_id"], context)}, ensure_ascii=False, indent=2) + "\n")
        if arguments.mode == "seed":
            os.kill(os.getpid(), signal.SIGTERM)
    finally:
        await ports.close()
        database.dispose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--mode", choices=["seed", "restore"], required=True)
    asyncio.run(main(parser.parse_args()))

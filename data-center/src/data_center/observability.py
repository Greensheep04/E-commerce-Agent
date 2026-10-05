import json
from contextlib import contextmanager
from datetime import UTC, datetime

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import InputDataset, Job, OutputDataset, Run, RunEvent, RunState
from openlineage.client.transport.file import FileConfig, FileTransport
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import ConsoleSpanExporter, SimpleSpanProcessor


class RunObserver:
    def __init__(self, directory, *, provider=None):
        directory.mkdir(parents=True, exist_ok=True)
        self.lineage = OpenLineageClient(transport=FileTransport(
            FileConfig(log_file_path=str(directory / "lineage.jsonl"), append=True)))
        self.output = (directory / "traces.jsonl").open("a", encoding="utf-8")
        self.provider = provider or TracerProvider(resource=Resource.create({"service.name": "data-center"}))
        self.provider.add_span_processor(SimpleSpanProcessor(ConsoleSpanExporter(
            out=self.output, formatter=lambda span: json.dumps(json.loads(span.to_json()),
                                                               ensure_ascii=False) + "\n")))
        self.tracer = self.provider.get_tracer("data-center")

    def event(self, run_id, operation, sources, state, snapshots=()):
        self.lineage.emit(RunEvent(
            eventTime=datetime.now(UTC).isoformat(), producer="data-center/0.1.0",
            run=Run(runId=run_id), job=Job(namespace="project-shop", name=operation),
            eventType=state,
            inputs=[InputDataset(namespace="shop-source", name=value) for value in sources],
            outputs=[OutputDataset(namespace="data-center-snapshot", name=value) for value in snapshots]))

    @contextmanager
    def running(self, task_id, run_id, operation, sources):
        self.event(run_id, operation, sources, RunState.START)
        with self.tracer.start_as_current_span("data-center.run", attributes={
            "task.id": task_id, "run.id": run_id, "operation": operation,
        }) as span:
            yield span

    def complete(self, run_id, operation, sources, snapshots):
        self.event(run_id, operation, sources, RunState.COMPLETE, snapshots)

    def fail(self, run_id, operation, sources):
        self.event(run_id, operation, sources, RunState.FAIL)

    def stop(self, run_id, operation, sources, *, cancelled=False):
        self.event(run_id, operation, sources, RunState.ABORT if cancelled else RunState.OTHER)

    def close(self):
        self.provider.shutdown()
        self.output.close()

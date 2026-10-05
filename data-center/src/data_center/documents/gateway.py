import math

from data_center_protocol import ModelGateway, ModelInvocation, ModelReply
from integration_protocol import CallContext


def embedding_batches(texts: list[str]) -> list[list[str]]:
    batches, current, size = [], [], 0
    for text in texts:
        length = len(text.encode())
        if length > 12000:
            raise ValueError("embedding单条输入超过宿主12000bytes范围")
        if current and (len(current) == 20 or size + length > 12000):
            batches.append(current)
            current, size = [], 0
        current.append(text)
        size += length
    if current:
        batches.append(current)
    return batches


class ModelStepPending(RuntimeError):
    def __init__(self, reply: ModelReply):
        super().__init__(f"模型请求尚未完成：{reply.status}，request_id={reply.request_id}")
        self.reply = reply


async def invoke_step(gateway: ModelGateway, request: ModelInvocation,
                      context: CallContext, *, before_invoke=None) -> ModelReply:
    reply = await gateway.lookup(request.task_id, request.step_key, context)
    if reply is None or reply.status == "reserved":
        if before_invoke is not None:
            before_invoke()
        reply = await gateway.invoke(request, context)
    if reply.status != "completed":
        raise ModelStepPending(reply)
    return reply


async def reuse_completed_step(gateway: ModelGateway, request: ModelInvocation, original: ModelReply,
                              context: CallContext, *, before_reuse=None) -> ModelReply:
    if original.status != "completed":
        raise ValueError("复用模型步骤需要completed真实响应")
    reply = await gateway.lookup(request.task_id, request.step_key, context)
    if reply is None:
        if before_reuse is not None:
            before_reuse()
        reply = await gateway.reuse(request, original.request_id, context)
    if reply.status != "completed":
        raise ModelStepPending(reply)
    if reply.request_id != original.request_id or reply.response != original.response:
        raise ValueError("复用模型步骤与原completed请求或响应不一致")
    return reply


def reply_vectors(reply: ModelReply, count: int) -> list[list[float]]:
    data = reply.response["data"]
    if len(data) != count or sorted(item["index"] for item in data) != list(range(count)):
        raise ValueError("embedding响应次序或数量无效")
    vectors = [item["embedding"] for item in sorted(data, key=lambda item: item["index"])]
    for vector in vectors:
        if len(vector) != 1024 or any(type(value) not in {float, int} or not math.isfinite(value) for value in vector):
            raise ValueError("embedding响应维数或数值无效")
        if not any(vector):
            raise ValueError("embedding响应模长为零")
    return vectors


def reply_content(reply: ModelReply) -> str:
    choice = reply.response["choices"][0]
    if choice["finish_reason"] != "stop":
        raise ValueError("模型响应未完整结束")
    content = choice["message"]["content"]
    if not isinstance(content, str) or not content.strip():
        raise ValueError("模型响应没有有效正文")
    return content

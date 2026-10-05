import asyncio
import json
from time import monotonic

import httpx
from sqlalchemy import select

from shop.budget import BudgetExceeded, BudgetService
from shop.customer_runtime_models import CustomerTurn
from shop.paid_models import UnknownRequest
from shop.providers import _provider_call
from shop.runtime_models import ProviderRequest


class CustomerModels:
    def __init__(self, customer):
        self.customer = customer
        self.budget = BudgetService(customer.maker)

    async def request(self, turn_id: str, step_key: str, service: str, body: dict,
                      timeout: float) -> dict:
        input_limit = len(json.dumps(body, ensure_ascii=False).encode('utf-8'))
        if input_limit > 131072:
            raise ValueError('客服模型输入超过费用预留范围')
        unknown_request = False
        resume_request_id = None
        with self.customer.maker.begin() as session:
            turn = session.get(CustomerTurn, turn_id, with_for_update=True)
            if turn is None or turn.cancel_requested or turn.status not in {'queued', 'running'}:
                raise PermissionError('消息已终止')
            records = session.scalars(select(ProviderRequest).where(
                ProviderRequest.run_id == turn_id, ProviderRequest.step_key.startswith(step_key))
                .order_by(ProviderRequest.created_at)).all()
            exact = [item for item in records if item.step_key == step_key
                     or item.step_key.startswith(step_key + '-retry-')]
            prior = exact[-1] if exact else None
            if prior and prior.request_body != body:
                raise ValueError('恢复请求内容与持久化记录不一致')
            if prior and (prior.service != service or prior.model != body['model']):
                raise ValueError('恢复请求的模型服务与原始记录不一致')
            if prior and prior.status == 'completed':
                return prior.response_body
            if prior and prior.status == 'sent':
                prior.status = 'unknown'
                prior.error = '进程中断后无法确认提供商用量'
            if (prior and prior.status == 'unknown'
                    and prior.price_basis.get('cluster_run_id') is not None):
                unknown_request = True
            elif prior and prior.status == 'reserved':
                request = prior
            elif (prior and prior.status == 'cancelled' and prior.error == 'cluster_stopped'
                    and prior.price_basis.get('cluster_run_id') is not None):
                resume_request_id = prior.id
            else:
                if service == 'deepseek' and turn.model_calls >= turn.config['model_limit']:
                    raise ValueError('达到三个模型请求上限')
                actual_step = step_key if prior is None else step_key + f'-retry-{len(exact)}'
                request = self.budget.reserve(
                    task_id=None, run_id=turn_id, step_key=actual_step, service=service,
                    model=body['model'], request_body=body, input_limit=input_limit,
                    output_limit=body.get('max_tokens', 0), verification=False, scope='customer',
                    session=session)
                if request.reservation_cny > 5:
                    raise BudgetExceeded('单次新增费用预留超过已授权范围')
                if service == 'deepseek':
                    turn.model_calls += 1
            if not unknown_request:
                request_id = resume_request_id or request.id
        if unknown_request:
            raise UnknownRequest('外部请求结果未知，保留费用预留并停止')
        if resume_request_id is not None:
            self.budget.resume_unsent(resume_request_id)
        self.budget.mark_sent(request_id)
        started = monotonic()
        try:
            response, provider_id = await _provider_call(self.customer.settings, service, body, timeout)
            response['client_duration_seconds'] = monotonic() - started
            self.budget.complete(request_id, response, provider_id)
        except httpx.ConnectError:
            self.budget.connection_failed(request_id)
            raise
        except asyncio.CancelledError:
            self.budget.mark_unknown(request_id, '本地请求已终止，提供商用量未知')
            raise
        except Exception as error:
            self.budget.mark_unknown(request_id, type(error).__name__)
            raise
        return response

    async def chat(self, turn_id: str, step: int, messages: list[dict], tools: list[dict],
                   timeout: float) -> dict:
        with self.customer.maker() as session:
            turn = session.get(CustomerTurn, turn_id)
            profile = turn.config
        body = {key: profile[key] for key in ('model', 'thinking', 'reasoning_effort', 'max_tokens')}
        body.update(messages=messages, tools=tools, stream=False)
        if 'response_format' in profile:
            body['response_format'] = profile['response_format']
        result = await self.request(turn_id, f'chat-{step}', 'deepseek', body, timeout)
        choice = result['choices'][0]
        if choice['finish_reason'] not in {'stop', 'tool_calls'}:
            raise ValueError('模型没有生成完整的客服响应')
        message = choice['message']
        if 'reasoning_content' not in message:
            raise ValueError('思考模式响应缺少reasoning_content')
        return message

    async def embed(self, turn_id: str, call_id: str, query: str, timeout: float) -> dict:
        return await self.request(turn_id, 'embed-' + call_id, 'dashscope', {
            'model': 'qwen3.7-text-embedding', 'input': [query], 'dimensions': 1024,
            'encoding_format': 'float'}, timeout)

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from shop.auth import current_user
from shop.models import User
from shop.orders import OrderQueryService

router = APIRouter(prefix="/api/customer", tags=["文字客服"])


def order_service(request: Request, user: User = Depends(current_user)):
    with request.app.state.sessionmaker() as session:
        try:
            service = OrderQueryService(session, user.id)
        except PermissionError as error:
            raise HTTPException(status_code=403, detail=str(error)) from error
        yield service


@router.get("/identity")
def identity(service: OrderQueryService = Depends(order_service)):
    return service.identity()


@router.get("/orders")
def orders(request: Request, order_number: str | None = None,
           start_date: date | None = None, end_date: date | None = None,
           product: str | None = None, limit: int = Query(20, ge=1, le=20),
           offset: int = Query(0, ge=0), service: OrderQueryService = Depends(order_service)):
    if set(request.query_params) - {"order_number", "start_date", "end_date", "product", "limit", "offset"}:
        raise HTTPException(status_code=422, detail="包含不支持的查询字段")
    try:
        return service.list_orders(order_number, start_date, end_date, product, limit, offset)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


@router.get("/orders/{order_number}")
def order(order_number: str, service: OrderQueryService = Depends(order_service)):
    try:
        result = service.get_order(order_number)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error
    if result is None:
        raise HTTPException(status_code=404, detail="当前账号无法查询该订单")
    return result


class CreateConversation(BaseModel):
    model_config = ConfigDict(extra='forbid')
    title: str = Field(default='客服咨询', min_length=1, max_length=200)


class SubmitMessage(BaseModel):
    model_config = ConfigDict(extra='forbid')
    request_id: str
    message: str = Field(min_length=1, max_length=4000)


class ConfirmTicket(BaseModel):
    model_config = ConfigDict(extra='forbid')
    request_id: str


def customer(request: Request):
    return request.app.state.customer


def customer_call(operation):
    try:
        return operation()
    except PermissionError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error


@router.get('/conversations')
def conversations(user: User = Depends(current_user), service=Depends(customer)):
    return customer_call(lambda: service.list_conversations(user.id))


@router.post('/conversations')
def create_conversation(body: CreateConversation, user: User = Depends(current_user),
                        service=Depends(customer)):
    return customer_call(lambda: service.create_conversation(user.id, body.title))


@router.get('/conversations/{conversation_id}')
def conversation(conversation_id: str, user: User = Depends(current_user), service=Depends(customer)):
    return customer_call(lambda: service.show_conversation(conversation_id, user.id))


@router.post('/conversations/{conversation_id}/messages')
async def message(conversation_id: str, body: SubmitMessage, request: Request,
                  user: User = Depends(current_user), service=Depends(customer)):
    turn = customer_call(lambda: service.submit(conversation_id, user.id, body.request_id, body.message))
    if turn['status'] in {'queued', 'running'}:
        request.app.state.customer_worker.schedule(turn['id'])
    return turn


@router.get('/conversations/{conversation_id}/events')
def events(conversation_id: str, after: int = Query(0, ge=0),
           user: User = Depends(current_user), service=Depends(customer)):
    return customer_call(lambda: service.events(conversation_id, user.id, after))


@router.get('/turns/{turn_id}')
def turn(turn_id: str, user: User = Depends(current_user), service=Depends(customer)):
    return customer_call(lambda: service.show_turn(turn_id, user.id))


@router.post('/turns/{turn_id}/cancel')
def cancel(turn_id: str, user: User = Depends(current_user), service=Depends(customer)):
    return customer_call(lambda: service.cancel(turn_id, user.id))


@router.post('/turns/{turn_id}/resume')
async def resume(turn_id: str, request: Request, user: User = Depends(current_user), service=Depends(customer)):
    turn = customer_call(lambda: service.resume(turn_id, user.id))
    if turn['status'] in {'queued', 'running'}:
        request.app.state.customer_worker.schedule(turn['id'])
    return turn


@router.post('/conversations/{conversation_id}/tickets/{pending_id}/confirm')
async def confirm(conversation_id: str, pending_id: str, body: ConfirmTicket, request: Request,
                  user: User = Depends(current_user), service=Depends(customer)):
    turn = customer_call(lambda: service.submit(conversation_id, user.id, body.request_id,
                                                '确认创建人工工单', confirm_pending=pending_id))
    if turn['status'] in {'queued', 'running'}:
        request.app.state.customer_worker.schedule(turn['id'])
    return turn


@router.get('/tickets/{number}')
def ticket(number: str, user: User = Depends(current_user), service=Depends(customer)):
    return customer_call(lambda: service.ticket(number, user.id))

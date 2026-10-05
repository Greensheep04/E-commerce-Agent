from fastapi import HTTPException, Request
from pwdlib import PasswordHash
from sqlalchemy import select

from shop.models import User

password_hash = PasswordHash.recommended()


def hash_password(password: str) -> str:
    return password_hash.hash(password)


def verify_password(password: str, encoded: str) -> bool:
    return password_hash.verify(password, encoded)


def current_user(request: Request) -> User:
    user_id = request.session.get("user_id")
    if user_id is None:
        raise HTTPException(status_code=401, detail="需要登录")
    with request.app.state.sessionmaker() as session:
        user = session.get(User, user_id)
        if user is None:
            raise HTTPException(status_code=401, detail="账号不存在")
        session.expunge(user)
        return user


def admin_user(request: Request) -> User:
    user = current_user(request)
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return user


def authenticate(session, username: str, password: str) -> User | None:
    user = session.scalar(select(User).where(User.username == username))
    if user is None or not verify_password(password, user.password_hash):
        return None
    return user

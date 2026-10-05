import csv
from pathlib import Path

from fastapi.testclient import TestClient

from shop.adventureworks import REVISION, import_orders
from shop.app import create_app
from shop.auth import hash_password
from shop.config import Settings
from shop.models import User
from shop.orders import bind_customer


def test_real_order_api_authentication_and_customer_isolation(db_session, data_dir):
    directory = Path('.local/data/orders') / REVISION
    import_orders(db_session, directory)
    users = [User(username=name, password_hash=hash_password('cus01-api-local-test'), role='customer')
             for name in ('cus01-api-first', 'cus01-api-second', 'cus01-api-unbound')]
    db_session.add_all(users)
    db_session.flush()
    bind_customer(db_session, users[0].id, 11091)
    bind_customer(db_session, users[1].id, 11176)
    with (directory / 'SalesOrderHeader.csv').open(encoding='utf-8', newline='') as source:
        headers = list(csv.reader(source, delimiter='\t'))
    first_number = next(row[7] for row in headers if row[10] == '11091')
    second_number = next(row[7] for row in headers if row[10] == '11176')
    settings = Settings(database_url=db_session.info['test_database_url'], data_dir=data_dir,
                        session_secret='cus01-api-test-session')
    app = create_app(settings)
    with TestClient(app) as client:
        assert client.get('/api/customer/orders').status_code == 401
        login = client.post('/api/login', json={'username': users[0].username,
                                               'password': 'cus01-api-local-test'})
        assert login.status_code == 200
        identity = client.get('/api/customer/identity').json()
        assert identity['customer_id'] == 11091
        assert identity['name'] == 'Dalton Perez'
        listing = client.get('/api/customer/orders')
        assert listing.status_code == 200
        assert listing.json()['total'] == 28
        assert len(listing.json()['orders']) == 20
        assert client.get(f'/api/customer/orders/{first_number}').status_code == 200
        other = client.get(f'/api/customer/orders/{second_number}')
        absent = client.get('/api/customer/orders/SO99999999')
        assert other.status_code == absent.status_code == 404
        assert other.json() == absent.json()
        assert client.get('/api/customer/orders/SO-invalid').status_code == 422
        assert client.get('/api/customer/orders?limit=0').status_code == 422
        assert client.get('/api/customer/orders?customer_id=11176').status_code == 422
        assert client.post('/api/logout').status_code == 200
        assert client.get('/api/customer/identity').status_code == 401
        assert client.post('/api/login', json={'username': users[2].username,
                                             'password': 'cus01-api-local-test'}).status_code == 200
        assert client.get('/api/customer/orders').status_code == 403

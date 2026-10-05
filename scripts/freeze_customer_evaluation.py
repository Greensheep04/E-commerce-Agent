import json
from collections import defaultdict
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from shop.adventureworks import REVISION, source_manifest
from shop.asset_index import MAIN_SOURCE
from shop.config import Settings
from shop.models import Asset, AssetVersion, User
from shop.orders import OrderQueryService
from shop.runtime_models import AssetIndex


def main():
    template_path = Path('tests/fixtures/customer_cases.json')
    template = json.loads(template_path.read_text())
    assert len(template['cases']) == 30 and template['repetitions'] == 3
    orders = {}
    identities = {}
    sequences = {}
    with Session(create_engine(Settings().database_url)) as session:
        for customer_id in (11091, 11176):
            user = session.scalar(select(User).where(User.username == f'customer-{customer_id}'))
            service = OrderQueryService(session, user.id)
            identities[str(customer_id)] = service.identity()
            listing = service.list_orders()
            pages = listing['orders'] + service.list_orders(offset=20)['orders']
            sequences[str(customer_id)] = [item['order_number'] for item in pages]
            orders[str(customer_id)] = {item['order_number']: service.get_order(item['order_number']) for item in pages}
        records = session.execute(select(Asset, AssetVersion, AssetIndex).join(
            AssetVersion, AssetVersion.asset_id == Asset.id).join(
            AssetIndex, AssetIndex.asset_version_id == AssetVersion.id).where(
                Asset.source_id == MAIN_SOURCE, AssetIndex.status == 'ready')
                .order_by(Asset.source_order)).all()
        products = {asset.id: {'asset_id': asset.id, 'title': version.title, 'version': version.version,
                    'original_fields': version.original_fields, 'source_id': asset.source_id,
                    'source_revision': asset.source_revision, 'record_id': asset.record_id,
                    'description': index.description} for asset, version, index in records}
    groups = defaultdict(list)
    for product in products.values():
        fields = product['original_fields']
        if fields.get('masterCategory') == 'Apparel':
            groups[tuple(str(fields[key]) for key in ('gender', 'articleType', 'baseColour'))].append(product['asset_id'])
    choices = sorted(groups, key=lambda key: (-len(groups[key]), key))
    a, b = choices[:2]
    conditions = lambda key: '、'.join(f'{field}为{value}' for field, value in zip(
        ('gender', 'articleType', 'baseColour'), key, strict=True))
    first_sequence = sequences['11091']
    long_order = max(orders['11091'], key=lambda number: orders['11091'][number]['item_count'])
    values = {'product_a_conditions': conditions(a), 'product_b_conditions': conditions(b),
              'order_first': first_sequence[0], 'order_second': first_sequence[1], 'order_long': long_order,
              'other_order': sequences['11176'][0], 'other_name': identities['11176']['name']}
    cases = json.loads(json.dumps(template['cases'], ensure_ascii=False))
    for case in cases:
        for step in case['steps']:
            if 'message' in step:
                step['message'] = step['message'].format(**values)
    source = source_manifest(Path('.local/data/orders') / REVISION)
    frozen = {**template, 'cases': cases, 'values': values, 'source': source, 'identities': identities,
              'orders': orders, 'sequences': sequences, 'products': products,
              'allowed_a': groups[a], 'allowed_b': groups[b],
              'template_sha256': sha256(template_path.read_bytes()).hexdigest(),
              'frozen_at': datetime.now(UTC).isoformat()}
    directory = Path('.scratch/evidence') / f'cus05-inputs-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid4().hex[:8]}'
    directory.mkdir(parents=True)
    output = directory / 'inputs.json'
    output.write_text(json.dumps(frozen, ensure_ascii=False, indent=2) + '\n')
    (directory / 'sha256.txt').write_text(sha256(output.read_bytes()).hexdigest() + '\n')
    print(str(output))


if __name__ == '__main__':
    main()

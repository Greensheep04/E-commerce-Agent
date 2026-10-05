import shutil
from pathlib import Path

import pytest

from shop.adventureworks import REVISION, load_source_rows

SOURCE_DIR = Path('.local/data/orders') / REVISION


def test_pinned_source_columns_and_actual_records():
    rows = load_source_rows(SOURCE_DIR)
    assert {name: len(records) for name, records in rows.items()} == {
        'SalesOrderHeader': 31465, 'SalesOrderDetail': 121317, 'Product': 504,
        'Customer': 19820, 'Person': 19972, 'Store': 701,
    }
    assert rows['SalesOrderHeader'][0][0] == '43659'
    assert rows['SalesOrderDetail'][0][4] == '776'
    assert rows['Product'][0][1] == 'Adjustable Race'
    customer = next(row for row in rows['Customer'] if row[0] == '11091')
    assert customer[1] == '4515'
    person = next(row for row in rows['Person'] if row[0] == '4515')
    assert person[4:7] == ['Dalton', '', 'Perez']


def test_changed_source_is_rejected(tmp_path):
    for source in SOURCE_DIR.glob('*.csv'):
        shutil.copyfile(source, tmp_path / source.name)
    with (tmp_path / 'Product.csv').open('ab') as output:
        output.write(b'\n')
    with pytest.raises(ValueError, match='摘要'):
        load_source_rows(tmp_path)


def test_missing_source_is_rejected(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_source_rows(tmp_path)

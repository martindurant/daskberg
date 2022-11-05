import os

import daskberg

here = os.path.dirname(os.path.abspath(__file__))
TEST_DIR = os.path.abspath(os.path.join(here, "..", "..", "test-data"))
ORIG_DIR = "/Users/mdurant/temp/warehouse/db/my_table"


def test1():
    i = daskberg.IcebergDataset(TEST_DIR + "/my_table", ORIG_DIR)
    df = i.read().compute()
    assert set(df.name) == {"Alex", "Bob", "Roger", "Fiona", "John"}
    assert df[df.email == "email@email.email"].iloc[0].to_dict() == {
        "name": "John",
        "age": 56,
        "email": "email@email.email",
    }
    assert df.email.value_counts().sum() == 1


def test2():
    i = daskberg.IcebergDataset(TEST_DIR + "/my_table", ORIG_DIR)
    assert i.version == 5
    i.open_snapshot()
    assert i.schema == [
        {"id": 1, "name": "name", "required": False, "type": "string"},
        {"id": 2, "name": "age", "required": False, "type": "int"},
        {"id": 3, "name": "email", "required": False, "type": "string"},
    ]
    i.open_snapshot(-1)
    assert i.schema == [
        {"id": 1, "name": "name", "required": False, "type": "string"},
        {"id": 2, "name": "age", "required": False, "type": "int"},
    ]

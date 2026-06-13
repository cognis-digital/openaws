"""Verify file-backed storage persists across App instances."""

from openaws.server import App


def test_data_persists_to_disk(tmp_path):
    data_dir = str(tmp_path / "store")
    app1 = App(data_dir)
    app1.s3.create_bucket("persist")
    app1.s3.put_object("persist", "k", b"value")
    app1.dynamodb.create_table("t", "id")
    app1.dynamodb.put_item("t", {"id": "1", "v": 9})
    app1.storage.close()

    app2 = App(data_dir)
    assert app2.s3.get_object("persist", "k")["body"] == b"value"
    assert app2.dynamodb.get_item("t", {"id": "1"})["v"] == 9
    app2.storage.close()

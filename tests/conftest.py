import pytest

from openaws.server import App, OpenAWSServer
from openaws.storage import Storage


@pytest.fixture()
def storage():
    s = Storage(":memory:")
    yield s
    s.close()


@pytest.fixture()
def app(storage):
    return App(storage=storage)


@pytest.fixture()
def server():
    """A real HTTP server running in-process on an ephemeral port."""
    srv = OpenAWSServer(host="127.0.0.1", port=0).start()
    yield srv
    srv.stop()

import pytest
from fastapi.testclient import TestClient

from api import main


@pytest.fixture()
def app():
    with TestClient(app=main.app) as client:
        yield client

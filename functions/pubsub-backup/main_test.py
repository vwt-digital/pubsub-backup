import flask
import pytest

import main


# Create a fake "app" for generating test request contexts.
@pytest.fixture(scope="module")
def app():
    return flask.Flask(__name__)


def test_handler_get(app):
    with app.test_request_context(data='some-subscription'):
        # res = main.handler(flask.request)
        main.handler(flask.request)
        # assert 'OK' in res

from got import __version__
from got.broker import InMemoryBroker
from got.got import Got
from got.task import BasicHTTPTask


def test_version():
    assert __version__ == '0.1.0'


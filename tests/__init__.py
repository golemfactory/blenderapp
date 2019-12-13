import pytest

# See https://stackoverflow.com/questions/41522767/pytest-assert-introspection-in-helper-function  # noqa
pytest.register_assert_rewrite('tests.simulationbase')

from typing import List

import pytest


@pytest.fixture()
def redis_servers() -> List[str]:
    return ['localhost:6379', 'localhost:6380']

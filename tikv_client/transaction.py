import asyncio
from .tikv_client import Client as AsyncClient


class Client:
    def __init__(self, pd_endpoint):
        self.inner = AsyncClient(pd_endpoint)

    def begin(self, pessimistic=False):
        event_loop = asyncio.get_event_loop()
        transaction = event_loop.run_until_complete(
            self.inner.begin(pessimistic))
        return Transaction(transaction)

    def current_timestamp(self):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(
            self.inner.current_timestamp())

    def snapshot(self, timestamp):
        snapshot = self.inner.snapshot(timestamp)
        return Snapshot(snapshot)


class Snapshot:
    def __init__(self, inner):
        self.inner = inner

    def get(self, key):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.get(key))

    def batch_get(self, keys):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.batch_get(keys))

    def scan(self, start, end=None, limit=1, include_start=True, include_end=False, key_only=False):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.scan(start, end, limit, include_start, include_end, key_only))


class Transaction:
    def __init__(self, inner):
        self.inner = inner

    def get(self, key):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.get(key))

    def get_for_update(self, key):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.get_for_update(key))

    def batch_get(self, keys):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.batch_get(keys))

    def batch_get_for_update(self, keys):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.batch_get_for_update(keys))

    def scan(self, start, end=None, limit=1, include_start=True, include_end=False, key_only=False):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.scan(start, end, limit, include_start, include_end, key_only))

    def lock_keys(self, keys):
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(self.inner.lock_keys(keys))

    def put(self, key, value):
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(self.inner.put(key, value))

    def delete(self, key):
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(self.inner.delete(key))

    def commit(self):
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(self.inner.commit())

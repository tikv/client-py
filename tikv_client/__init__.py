# Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

import asyncio
from . import asynchronous


class RawClient:
    def __init__(self, pd_endpoint):
        self.inner = asynchronous.RawClient(pd_endpoint)

    def get(self, key, cf="default"):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.get(key, cf))

    def batch_get(self, keys, cf="default"):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.batch_get(keys, cf))

    def scan(self, start, end, limit, include_start=True, include_end=False, cf="default"):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.scan(start, end, limit, include_start, include_end, cf))
   
    def scan_keys(self, start, end, limit, include_start=True, include_end=False, cf="default"):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.scan_keys(start, end, limit, include_start, include_end, cf))

    def put(self, key, value, cf="default"):
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(self.inner.put(key, value, cf))

    def batch_put(self, pairs, cf="default"):
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(self.inner.put(pairs, cf))

    def delete(self, key, cf="default"):
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(self.inner.delete(key, cf))

    def batch_delete(self, keys, cf="default"):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.batch_delete(keys, cf))

    def delete_range(self, start, end=None, include_start=True, include_end=False, cf="default"):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.delete_range(start, end, include_start, include_end, cf))


class TransactionClient:
    def __init__(self, pd_endpoint):
        self.inner = asynchronous.TransactionClient(pd_endpoint)

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

    def scan(self, start, end, limit, include_start=True, include_end=False):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.scan(start, end, limit, include_start, include_end))

    def scan_keys(self, start, end, limit, include_start=True, include_end=False):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.scan_keys(start, end, limit, include_start, include_end))


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

    def scan(self, start, end, limit, include_start=True, include_end=False):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.scan(start, end, limit, include_start, include_end))

    def scan_keys(self, start, end, limit, include_start=True, include_end=False):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.scan_keys(start, end, limit, include_start, include_end))

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

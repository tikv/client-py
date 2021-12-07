# Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

import asyncio
from . import asynchronous


class RawClient:
    def __init__(self):
        raise Exception("Please use `RawClient.connect()` instead.")

    @classmethod
    def connect(cls, pd_endpoint):
        event_loop = asyncio.get_event_loop()
        inner = event_loop.run_until_complete(
            asynchronous.RawClient.connect(pd_endpoint))
        self = cls.__new__(cls)
        self.inner = inner
        return self

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
        event_loop.run_until_complete(self.inner.batch_put(pairs, cf))

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
    def __init__(self):
        raise Exception("Please use `TransactionClient.connect()` instead.")

    @classmethod
    def connect(cls, pd_endpoint):
        event_loop = asyncio.get_event_loop()
        inner = event_loop.run_until_complete(
            asynchronous.TransactionClient.connect(pd_endpoint))
        self = cls.__new__(cls)
        self.inner = inner
        return self

    def begin(self, pessimistic=False):
        event_loop = asyncio.get_event_loop()
        transaction = event_loop.run_until_complete(
            self.inner.begin(pessimistic))
        return Transaction(transaction)

    def current_timestamp(self):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(
            self.inner.current_timestamp())

    def snapshot(self, timestamp, pessimistic):
        snapshot = self.inner.snapshot(timestamp, pessimistic)
        return Snapshot(snapshot)


class Snapshot:
    def __init__(self, inner):
        self.inner = inner

    def get(self, key):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.get(key))

    def key_exists(self, key):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.key_exists(key))

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

    def key_exists(self, key):
        event_loop = asyncio.get_event_loop()
        return event_loop.run_until_complete(self.inner.key_exists(key))

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

    def insert(self, key, value):
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(self.inner.insert(key, value))

    def delete(self, key):
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(self.inner.delete(key))

    def commit(self):
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(self.inner.commit())

    def rollback(self):
        event_loop = asyncio.get_event_loop()
        event_loop.run_until_complete(self.inner.rollback())

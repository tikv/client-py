# Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

from .. import tikv_client


class RawClient:
    def __init__(self):
        raise Exception("Please use `RawClient.connect()` instead.")

    @classmethod
    async def connect(cls, pd_endpoint):
        inner = await tikv_client.RawClient.connect(pd_endpoint)
        self = cls.__new__(cls)
        self.inner = inner
        return self

    async def get(self, key, cf="default"):
        return await self.inner.get(key, cf)

    async def batch_get(self, keys, cf="default"):
        return await self.inner.batch_get(keys, cf)

    async def scan(self, start, end, limit, include_start=True, include_end=False, cf="default"):
        return await self.inner.scan(start, end, limit, include_start, include_end, cf)

    async def scan_keys(self, start, end, limit, include_start=True, include_end=False, cf="default"):
        return await self.inner.scan_keys(start, end, limit, include_start, include_end, cf)

    async def put(self, key, value, cf="default"):
        await self.inner.put(key, value, cf)

    async def batch_put(self, pairs, cf="default"):
        await self.inner.put(pairs, cf)

    async def delete(self, key, cf="default"):
        await self.inner.delete(key, cf)

    async def batch_delete(self, keys, cf="default"):
        return await self.inner.batch_delete(keys, cf)

    async def delete_range(self, start, end=None, include_start=True, include_end=False, cf="default"):
        return await self.inner.delete_range(start, end, include_start, include_end, cf)


class TransactionClient:
    def __init__(self):
        raise Exception("Please use `TransactionClient.connect()` instead.")

    @classmethod
    async def connect(cls, pd_endpoint):
        inner = await tikv_client.TransactionClient.connect(pd_endpoint)
        self = cls.__new__(cls)
        self.inner = inner
        return self

    async def begin(self, pessimistic=False):
        transaction = await self.inner.begin(pessimistic)
        return Transaction(transaction)

    async def current_timestamp(self):
        return await self.inner.current_timestamp()

    def snapshot(self, timestamp, pessimistic):
        snapshot = self.inner.snapshot(timestamp, pessimistic)
        return Snapshot(snapshot)


class Snapshot:
    def __init__(self, inner):
        self.inner = inner

    async def get(self, key):
        return await self.inner.get(key)

    async def key_exists(self, key):
        return await self.inner.key_exists(key)

    async def batch_get(self, keys):
        return await self.inner.batch_get(keys)

    async def scan(self, start, end, limit, include_start=True, include_end=False):
        return await self.inner.scan(start, end, limit, include_start, include_end)

    async def scan_keys(self, start, end, limit, include_start=True, include_end=False):
        return await self.inner.scan_keys(start, end, limit, include_start, include_end)


class Transaction:
    def __init__(self, inner):
        self.inner = inner

    async def get(self, key):
        return await self.inner.get(key)

    async def get_for_update(self, key):
        return await self.inner.get_for_update(key)

    async def key_exists(self, key):
        return await self.inner.key_exists(key)

    async def batch_get(self, keys):
        return await self.inner.batch_get(keys)

    async def batch_get_for_update(self, keys):
        return await self.inner.batch_get_for_update(keys)

    async def scan(self, start, end, limit, include_start=True, include_end=False):
        return await self.inner.scan(start, end, limit, include_start, include_end)

    async def scan_keys(self, start, end, limit, include_start=True, include_end=False):
        return await self.inner.scan_keys(start, end, limit, include_start, include_end)

    async def lock_keys(self, keys):
        await self.inner.lock_keys(keys)

    async def put(self, key, value):
        await self.inner.put(key, value)

    async def insert(self, key, value):
        await self.inner.insert(key, value)

    async def delete(self, key):
        await self.inner.delete(key)

    async def commit(self):
        await self.inner.commit()

    async def rollback(self):
        await self.inner.rollback()

from . import transaction

# from tikv_client.Transaction import Client

# class Client:
#     def __init__(self, pd_endpoint):
#         self.inner = ClientInner(pd_endpoint)
    
#     async def begin(self):
#         return Transaction(await self.inner.begin())

# class Transaction:
#     def __init__(self, inner):
#         self.inner = inner
    
#     async def get(self, key):
#         return await self.inner.get(key)

#     async def get_for_update(self, key):
#         return await self.inner.get_for_update(key)

#     async def batch_get(self, keys):
#         return await self.inner.batch_get(keys)

#     async def batch_get_for_update(self, keys):
#         return await self.inner.batch_get_for_update(keys)

#     async def scan(self, start, stop, limit):
#         return await self.inner.scan(start, stop, limit)

#     async def lock_keys(self, keys):
#         await self.inner.lock_keys(keys)

#     async def put(self, key, val):
#         await self.inner.put(key, val)

#     async def delete(self, key):
#         await self.inner.delete(key)

#     async def commit(self):
#         await self.inner.commit()

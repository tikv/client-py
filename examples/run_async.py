import asyncio
from tikv_client.asynchronous import TransactionClient

async def main():
    client = TransactionClient("127.0.0.1:2379")

    txn = await client.begin()
    await txn.put(b"k1", b"v1")
    await txn.put(b"k2", b"v2")
    await txn.put(b"k3", b"v3")
    await txn.put(b"k4", b"v4")
    await txn.put(b"k5", b"v5")
    await txn.commit()

    snapshot = client.snapshot(await client.current_timestamp())
    print(await snapshot.get(b"k3"))
    print(await snapshot.batch_get([b"k1", b"k4"]))
    print(await snapshot.scan(b"k1", end=None, limit=10, include_start=False))

event_loop = asyncio.get_event_loop()
asyncio.get_event_loop().run_until_complete(main())

import pytest

from tikv_client import RawClient
from tikv_client.asynchronous import TransactionClient


def test_raw_client():
    client = RawClient.connect(pd_endpoints=["0.0.0.0:2379"])

    client.put(b"k1", b"v1")
    client.put(b"k2", b"v2")
    client.put(b"k3", b"v3")
    client.batch_put({b"k4": b"v4", b"k5": b"v5"})

    assert client.get(b"k3") == b"v3"
    assert client.batch_get([b"k1", b"k4"]), [b"v1", b"v4"]
    scan = client.scan(b"k1", end=None, limit=10, include_start=False)

    assert scan[:4] == [
        (b"k2", b"v2"),
        (b"k3", b"v3"),
        (b"k4", b"v4"),
        (b"k5", b"v5"),
    ]


@pytest.mark.asyncio
async def test_async_raw_client():
    client = await TransactionClient.connect(["127.0.0.1:2379"])

    txn = await client.begin(pessimistic=True)
    try:
        await txn.put(b"k1", b"v1")
        await txn.put(b"k2", b"v2")
        await txn.put(b"k3", b"v3")
        await txn.put(b"k4", b"v4")
        await txn.put(b"k5", b"v5")
    finally:
        await txn.commit()

    txn = await client.begin(pessimistic=True)
    try:
        assert await txn.get(b"k3") == b"v3"
        assert await txn.batch_get([b"k1", b"k4"]), [b"v1", b"v4"]
    finally:
        await txn.commit()

    snapshot = client.snapshot(await client.current_timestamp(), pessimistic=True)
    assert await snapshot.get(b"k3") == b"v3"
    assert await snapshot.batch_get([b"k1", b"k4"]) == [(b"k1", b"v1"), (b"k4", b"v4")]

    scan = await snapshot.scan(b"k1", end=None, limit=10, include_start=False)
    assert scan[:4] == [
        (b"k2", b"v2"),
        (b"k3", b"v3"),
        (b"k4", b"v4"),
        (b"k5", b"v5"),
    ]

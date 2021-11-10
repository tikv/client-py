import pytest
from tikv_client.asynchronous import TransactionClient


@pytest.mark.asyncio
async def test_tikv_driver(tikvd):
    url = f"{tikvd[0]}:{tikvd[2]}"

    driver = await TransactionClient.connect(url)

    txn = await driver.begin(pessimistic=False)
    await txn.put(b"/internal/kbs/kb1/title", b"My title")
    await txn.put(b"/internal/kbs/kb1/shards/shard1", b"node1")

    await txn.put(b"/kbs/kb1/r/uuid1/text", b"My title")

    result = await txn.get(b"/kbs/kb1/r/uuid1/text")
    assert result == b"My title"

    await txn.commit()

    txn = await driver.begin(pessimistic=False)
    result = await txn.get(b"/kbs/kb1/r/uuid1/text")
    assert result == b"My title"

    result = await txn.batch_get(
        [b"/kbs/kb1/r/uuid1/text", b"/internal/kbs/kb1/shards/shard1"]
    )
    await txn.put(b"/internal/kbs/kb1/shards/shard2", b"node1")

    await txn.rollback()

    txn = await driver.begin(pessimistic=False)
    for keys in await txn.scan_keys(start=b"/internal", end=b"/internal", limit=10):
        assert keys in [b"/internal/kbs/kb1/title", b"/internal/kbs/kb1/shards/shard1"]
    await txn.rollback()


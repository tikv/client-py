from tikv_client import TransactionClient

client = TransactionClient("127.0.0.1:2379")

txn = client.begin()
txn.put(b"k1", b"v1")
txn.put(b"k2", b"v2")
txn.put(b"k3", b"v3")
txn.put(b"k4", b"v4")
txn.put(b"k5", b"v5")
txn.commit()

txn2 = client.begin()
print(txn2.get(b"k3"))
print(txn2.batch_get([b"k1", b"k4"]))
print(txn2.scan(b"k1", limit=10, include_start=False))

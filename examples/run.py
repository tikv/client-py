from tikv_client import TransactionClient

client = TransactionClient.connect("127.0.0.1:2379")

txn = client.begin(pessimistic=True)
txn.put(b"k1", b"v1")
txn.put(b"k2", b"v2")
txn.put(b"k3", b"v3")
txn.put(b"k4", b"v4")
txn.put(b"k5", b"v5")
txn.commit()

snapshot = client.snapshot(client.current_timestamp(), pessimistic=True)
print(snapshot.get(b"k3"))
print(snapshot.batch_get([b"k1", b"k4"]))

for k, v in snapshot.scan(b"k1", end=None, limit=10, include_start=False):
    print(k, v)

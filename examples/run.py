from tikv_client import RawClient

client = RawClient.connect("127.0.0.1:2379")

client.put(b"k1", b"v1")
client.put(b"k2", b"v2")
client.put(b"k3", b"v3")
client.put(b"k4", b"v4")
client.put(b"k5", b"v5")

print(client.get(b"k3"))
print(client.batch_get([b"k1", b"k4"]))

for k, v in client.scan(b"k1", end=None, limit=10, include_start=False):
    print(k, v)

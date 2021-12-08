from tikv_client import RawClient
import time

client = RawClient.connect(["127.0.0.1:2379"])

client.put(b"k1", b"v1")
client.put(b"k2", b"v2")
client.put(b"k3", b"v3", ttl_secs=2)
client.batch_put({b"k4": b"v4", b"k5": b"v5"})
client.batch_put_with_ttl({b"k6": (b"v6", 3)})

print(client.get(b"k3"))
print(client.batch_get([b"k1", b"k4"]))

print("k3 ttl:", client.get_key_ttl_secs(b"k3"))
print("k6 ttl:", client.get_key_ttl_secs(b"k6"))
time.sleep(1)
print("k3 ttl:", client.get_key_ttl_secs(b"k3"))
print("k6 ttl:", client.get_key_ttl_secs(b"k6"))
time.sleep(1)
print("k3 ttl:", client.get_key_ttl_secs(b"k3"))
print("k6 ttl:", client.get_key_ttl_secs(b"k6"))

for k, v in client.scan(b"k1", end=None, limit=10, include_start=False):
    print(k, v)

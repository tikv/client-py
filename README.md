# TiKV Client (Python)

![Build and publish](https://github.com/tikv/client-py/workflows/Build%20and%20publish/badge.svg)

This library is a TiKV client in Python; it supports both synchronous and asynchronous API. 

It's built on top of 
[TiKV Client in Rust](https://github.com/tikv/client-rust) via 
CFFI and [PyO3 Python binding](https://github.com/PyO3/pyo3). 

This client is still in the stage of prove-of-concept and under heavy development.

## Install

The package requires Python 3.5+.

```
pip3 install -i https://test.pypi.org/simple/ tikv-client
```

## Install (Development)

```
> pip3 install maturin

> maturin build
🍹 Building a mixed python/rust project
🔗 Found pyo3 bindings
🐍 Found CPython 3.8 at python3.8
📦 Built source distribution to /home/andy/Code/client-py/target/wheels/tikv_client-0.1.0.tar.gz
    Blocking waiting for file lock on build directory
   Compiling pyo3 v0.12.3
   Compiling tikv-client v0.1.0 (/home/andy/Code/client-py)
    Finished dev [unoptimized + debuginfo] target(s) in 17.62s
📦 Built wheel for CPython 3.8 to /home/andy/Code/client-py/target/wheels/tikv_client-0.1.0-cp38-cp38-manylinux1_x86_64.whl

> pip3 install target/wheels/tikv_client-0.1.0-cp38-cp38-manylinux1_x86_64.whl
Installing collected packages: tikv-client
Successfully installed tikv-client-0.1.0
```

## Example

Python TiKV client is synchronous by defult:

```python
from tikv_client.transaction import Client

client = Client("127.0.0.1:2379")

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
```

Asynchronous client is available in `tikv_client.asynchronous` module. Modules and classes under this modules is similar to the synchronous ones.

```python
import asyncio
from tikv_client.asynchronous.transaction import Client

async def main():
    client = Client("127.0.0.1:2379")

    txn = await client.begin()
    await txn.put(b"k1", b"v1")
    await txn.put(b"k2", b"v2")
    await txn.put(b"k3", b"v3")
    await txn.put(b"k4", b"v4")
    await txn.put(b"k5", b"v5")
    await txn.commit()

    txn2 = await client.begin()
    print(await txn2.get(b"k3"))
    print(await txn2.batch_get([b"k1", b"k4"]))
    print(await txn2.scan(b"k1", limit=10, include_start=False))

event_loop = asyncio.get_event_loop()
asyncio.get_event_loop().run_until_complete(main())
```

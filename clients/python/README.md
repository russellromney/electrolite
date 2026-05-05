# Electrolite Python Client

Tiny synchronous Python client for consuming an Electrolite HTTP Shape.

It is useful for scripts, tests, workers, or small Python services that
want the same snapshot-plus-replay lifecycle as the browser client.

```py
from electrolite import ShapeClient

client = ShapeClient("http://localhost:3000/electrolite/v1/projectTodos/p1")

client.request(offset=-1)
print(client.current_rows())

client.request(offset=client.offset, live=True)
print(client.current_rows())
```

The client:

- reads the initial snapshot
- stores rows by primary key
- applies insert, update, and delete messages
- handles `204` live timeouts as "no change"
- handles `409 resync_required` by fetching a fresh snapshot
- stages bounded replay pages until `up_to_date`

Run tests:

```sh
cd clients/python
python3 -m unittest
```

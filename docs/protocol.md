# Protocol Sketch

The first protocol should be boring HTTP.

## Initial Sync

```http
GET /electrolite/v1/shape/:name?offset=-1
```

The server:

1. authorizes the named shape for the current user
2. records the current log high-water mark
3. queries current rows for the shape
4. returns insert messages for the rows
5. returns an `up-to-date` control message with the continuation offset

Example response:

```json
[
  {
    "headers": { "operation": "insert" },
    "key": "[7]",
    "value": { "id": 7, "title": "ship electrolite", "done": false }
  },
  {
    "headers": { "control": "up-to-date" },
    "offset": 124
  }
]
```

## Live Long-Poll

```http
GET /electrolite/v1/shape/:name?offset=124&live=true
```

The server:

1. returns immediately if new messages exist
2. otherwise waits up to a bounded timeout
3. returns `204 No Content` if nothing changes
4. lets the client reconnect at the latest offset

Long-polling keeps the endpoint HTTP/CDN friendly. WebSockets can come
later as an adapter, not the core protocol.

## Membership Transitions

For each log row:

```text
old matches? new matches?

false -> true   insert
true  -> true   update
true  -> false  delete
false -> false  ignore
```

## Resync

If a client asks for an offset older than retained history:

```http
409 Conflict
```

```json
{ "error": "resync_required" }
```

The client restarts with `offset=-1`.

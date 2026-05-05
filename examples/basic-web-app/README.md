# Basic Web App

The smallest real web app example:

- Node-style TypeScript surface through the repo-local Electrolite
  package
- pure Node backend, so the demo does not require Rust or a native build
- SQLite database in a temp directory
- Electrolite route mounted at `/electrolite/v1/...`
- browser `ShapeClient`
- a left column that writes todos to SQLite
- a right column that subscribes to the Electrolite Shape and updates live

Run from the repository root:

```sh
npm run demo:web
```

Then open:

```text
http://localhost:3000
```

Click **Add typed todo** or **Add random todo** in the left column. The
POST writes to SQLite through the embedded Electrolite Node package. The
right column is a separate browser subscriber, and it updates through the
live Shape subscription.

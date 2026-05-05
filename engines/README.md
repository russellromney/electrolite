# Electrolite engines

Electrolite is meant to be a tiny protocol with tiny embedded engines.
The browser protocol stays the same; each backend language just installs
SQLite triggers, serves snapshots, and replays live logical changes.

- [TypeScript / Node](../packages/electrolite-node/README.md) is the main engine.
- [Python](python/README.md) is a small stdlib `sqlite3` engine for Flask-style apps.

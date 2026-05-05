"""Tiny aspirational API sketch."""

from electrolite import Shape


active_todos = Shape(
    name="activeTodos",
    table="todos",
    where={"project_id": "p1", "done": False},
    columns=["id", "title", "done", "project_id"],
)

print(active_todos.handle)

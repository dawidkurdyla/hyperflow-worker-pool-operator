# Resolves the effective queue settings. Defaults to using spec.taskType as name
# when spec.queue.name is not provided.

import os

def resolve_queue(spec: dict) -> dict:
    q = (spec.get('queue') or {})
    name = q.get('name') or spec.get('taskType')  # default queue name <- taskType
    if not name:
        raise ValueError("Cannot resolve queue name (need spec.taskType or spec.queue.name).")
    return {
        "name": name,
        "vhost": q.get("vhost") or os.getenv("RABBIT_VHOST", "/"),
        "durable": q.get("durable", True),
        "autoDelete": q.get("autoDelete", False),
        "arguments": q.get("arguments") or {},
    }

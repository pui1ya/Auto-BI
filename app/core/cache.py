import json
import hashlib
from typing import Any, Optional
from app.core.config import settings

# In-memory fallback when Redis is not configured
_memory_cache: dict[str, Any] = {}


def _make_key(namespace: str, params: dict) -> str:
    raw = json.dumps(params, sort_keys=True)
    h = hashlib.md5(raw.encode()).hexdigest()
    return f"{namespace}:{h}"


def cache_get(namespace: str, params: dict) -> Optional[Any]:
    key = _make_key(namespace, params)
    return _memory_cache.get(key)


def cache_set(namespace: str, params: dict, value: Any, ttl: int = settings.CACHE_TTL):
    key = _make_key(namespace, params)
    _memory_cache[key] = value


def cache_clear(namespace: Optional[str] = None):
    if namespace is None:
        _memory_cache.clear()
    else:
        keys_to_del = [k for k in _memory_cache if k.startswith(namespace)]
        for k in keys_to_del:
            del _memory_cache[k]
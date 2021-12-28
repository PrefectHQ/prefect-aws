import datetime
from typing import Any, Callable, Dict, Iterable, Optional, Union
from dataclasses import dataclass

from prefect.context import TaskRunContext

@dataclass
class TaskArgs:
    name: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[Iterable[str]] = None
    cache_key_fn: Optional[
        Callable[["TaskRunContext", Dict[str, Any]], Optional[str]]
    ] = None
    cache_expiration: Optional[datetime.timedelta] = None
    retries: Optional[int] = 0
    retry_delay_seconds: Optional[Union[float, int]] = 0

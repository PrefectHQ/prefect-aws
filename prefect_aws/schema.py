from abc import ABC
import datetime
from typing import Any, Callable, Dict, Iterable, Optional, Union
from dataclasses import dataclass

from prefect.context import TaskRunContext
from prefect.task_runners import BaseTaskRunner, SequentialTaskRunner


class BaseDefaultValues(ABC):
    ...


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


@dataclass
class FlowArgs:
    name: Optional[str] = None
    version: Optional[str] = None
    task_runner: BaseTaskRunner = SequentialTaskRunner
    description: Optional[str] = None
    timeout_seconds: Optional[Union[int, float]] = None
    validate_parameters: bool = True

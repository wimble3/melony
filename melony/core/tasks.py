import json

from datetime import datetime
from dataclasses import asdict, dataclass, field
from inspect import unwrap
from typing import (
    Callable,
    Any,
    TYPE_CHECKING,
    final,
    Final
)


if TYPE_CHECKING:
    from melony.core.brokers import BaseBroker

type Task = AsyncTask | SyncTask


_MAX_COUNTDOWN_SEC: Final[int] = 900
_MAX_COUNTDOWN_MIN: Final[float] = _MAX_COUNTDOWN_SEC / 60


@final
@dataclass(kw_only=True)
class _TaskMeta:
    retries_left: int | None = None
    timestamp: float = field(default_factory=lambda: datetime.timestamp(datetime.now()))


@dataclass(frozen=True, kw_only=True)
class _BaseTask:
    task_id: str
    kwargs: dict[str, Any]
    countdown: int
    retries: int
    retry_timeout: int

    _meta: _TaskMeta = field(default_factory=_TaskMeta)

    @final
    def __post_init__(self) -> None:
        self._validate_countdown()
        self._set_retries_left_to_meta()

    @final
    @property
    def timestamp(self) -> float:
        return self._meta.timestamp

    @final
    def get_execution_timestamp(self) -> float:
        return self.timestamp + self.countdown

    @final
    def _validate_countdown(self) -> None:
        if self.countdown < 0:
            raise ValueError("Countdown cannot be negative")
        elif self.countdown >= _MAX_COUNTDOWN_SEC:
            raise ValueError(
                f"Countdown cannot be greater than {_MAX_COUNTDOWN_SEC} "
                f"({_MAX_COUNTDOWN_MIN} minutes)"
            )

    @final
    def _set_retries_left_to_meta(self) -> None:
        if not self._meta.retries_left:
            self._meta.retries_left = self.retries


@final
@dataclass(frozen=True, kw_only=True)
class _TaskJSONSerializable(_BaseTask):
    func_name: str
    func_path: str

@dataclass(frozen=True, kw_only=True)
class _SerializableTask(_BaseTask):
    func: Callable
    func_path: str
    broker: "BaseBroker"

    @final
    def as_json_serializable_obj(self) -> _TaskJSONSerializable:
        return _TaskJSONSerializable(
            task_id=self.task_id,
            kwargs=self.kwargs,
            countdown=self.countdown,
            func_name=self.func.__name__,
            func_path=self.func_path,
            retries=self.retries,
            retry_timeout=self.retry_timeout,
            _meta=self._meta
        )

    @final
    def as_dict(self) -> dict[str, Any]:
        return asdict(self.as_json_serializable_obj())

    @final
    def as_json(self) -> str:
        json_str = json.dumps(self.as_dict())
        return json_str


@final
@dataclass(frozen=True, kw_only=True)
class AsyncTask(_SerializableTask):
    async def execute(self) -> Any:
        unwrapped_func = unwrap(self.func)
        task_result = await unwrapped_func(**self.kwargs)
        return task_result

@final
@dataclass(frozen=True, kw_only=True)
class SyncTask(_SerializableTask):
    def execute(self) -> Any:
        unwrapped_func = unwrap(self.func)
        task_result = unwrapped_func(**self.kwargs)
        return task_result
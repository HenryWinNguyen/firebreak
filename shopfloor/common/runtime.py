"""Runtime-mutable settings, exposed through /admin/chaos. Values are type-checked."""

from __future__ import annotations

from typing import Any


class UnknownSettingError(KeyError):
    """Raised when a setting name was never declared."""


class InvalidSettingError(ValueError):
    """The value has the wrong type or is out of range for the setting."""


class RuntimeSettings:
    def __init__(
        self,
        defaults: dict[str, Any],
        bounds: dict[str, tuple[float | None, float | None]] | None = None,
    ) -> None:
        self._defaults = dict(defaults)
        self._values = dict(defaults)
        self._bounds = bounds or {}

    def _coerce(self, name: str, value: Any) -> Any:
        default = self._defaults[name]
        if isinstance(default, bool):
            ok = isinstance(value, bool)
        elif isinstance(default, int):
            ok = isinstance(value, int) and not isinstance(value, bool)
        elif isinstance(default, float):
            ok = isinstance(value, int | float) and not isinstance(value, bool)
            value = float(value) if ok else value
        else:
            ok = isinstance(value, str) and value != ""
        if not ok:
            raise InvalidSettingError(f"{name} expects {type(default).__name__}, got {value!r}")
        low, high = self._bounds.get(name, (None, None))
        if (low is not None and value < low) or (high is not None and value > high):
            raise InvalidSettingError(f"{name}={value!r} outside [{low}, {high}]")
        return value

    def get(self, name: str) -> Any:
        try:
            return self._values[name]
        except KeyError:
            raise UnknownSettingError(name) from None

    def set(self, name: str, value: Any) -> None:
        if name not in self._defaults:
            raise UnknownSettingError(name)
        self._values[name] = self._coerce(name, value)

    def reset(self) -> None:
        self._values = dict(self._defaults)

    def snapshot(self) -> dict[str, Any]:
        return dict(self._values)

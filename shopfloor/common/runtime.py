"""Runtime-mutable settings. T-012 exposes these through /admin/chaos."""

from __future__ import annotations

from typing import Any


class UnknownSettingError(KeyError):
    """Raised when a setting name was never declared."""


class RuntimeSettings:
    def __init__(self, defaults: dict[str, Any]) -> None:
        self._defaults = dict(defaults)
        self._values = dict(defaults)

    def get(self, name: str) -> Any:
        try:
            return self._values[name]
        except KeyError:
            raise UnknownSettingError(name) from None

    def set(self, name: str, value: Any) -> None:
        if name not in self._defaults:
            raise UnknownSettingError(name)
        self._values[name] = value

    def reset(self) -> None:
        self._values = dict(self._defaults)

    def snapshot(self) -> dict[str, Any]:
        return dict(self._values)

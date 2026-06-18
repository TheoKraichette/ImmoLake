"""Tiny Streamlit compatibility layer for non-Streamlit test environments."""
from __future__ import annotations

from typing import Callable, TypeVar

F = TypeVar("F", bound=Callable)


class _NoopStreamlit:
    def cache_data(self, *args, **kwargs):
        return _decorator_or_function(args)

    def cache_resource(self, *args, **kwargs):
        return _decorator_or_function(args)


def _decorator_or_function(args):
    if args and callable(args[0]):
        return args[0]

    def decorator(func: F) -> F:
        return func

    return decorator


try:
    import streamlit as st
except ModuleNotFoundError:
    st = _NoopStreamlit()

"""Shared immutable filter state."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Filters:
    regions: tuple[str, ...] = ()
    departements: tuple[str, ...] = ()
    communes: tuple[str, ...] = ()
    types_bien: tuple[str, ...] = ()
    etiquettes: tuple[str, ...] = ()
    passoires_only: bool = False
    prix_m2_min: int | None = None
    prix_m2_max: int | None = None
    surface_min: int | None = None
    surface_max: int | None = None
    nb_dpe_min: int = 30
    opportunite_k: float = 1.2

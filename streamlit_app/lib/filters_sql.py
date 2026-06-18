"""Safe SQL WHERE builder for DuckDB queries."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from lib.filter_state import Filters


@dataclass(frozen=True)
class WhereClause:
    sql: str
    params: tuple[Any, ...]


def _add_in_clause(
    clauses: list[str],
    params: list[Any],
    column: str,
    values: tuple[str, ...],
) -> None:
    if not values:
        return
    placeholders = ", ".join("?" for _ in values)
    clauses.append(f"{column} IN ({placeholders})")
    params.extend(values)


def build_where(filters: Filters, *, alias: str = "m") -> WhereClause:
    """Build a parameterized WHERE clause; user values never enter SQL text."""
    prefix = f"{alias}." if alias else ""
    clauses: list[str] = []
    params: list[Any] = []

    _add_in_clause(clauses, params, f"{prefix}region", filters.regions)
    _add_in_clause(clauses, params, f"{prefix}departement", filters.departements)
    _add_in_clause(clauses, params, f"{prefix}commune", filters.communes)
    _add_in_clause(clauses, params, f"{prefix}type_bien", filters.types_bien)
    _add_in_clause(clauses, params, f"{prefix}etiquette", filters.etiquettes)

    if filters.passoires_only:
        clauses.append(f"{prefix}etiquette IN ('F', 'G')")
    if filters.prix_m2_min is not None:
        clauses.append(f"{prefix}prix_m2 >= ?")
        params.append(filters.prix_m2_min)
    if filters.prix_m2_max is not None:
        clauses.append(f"{prefix}prix_m2 <= ?")
        params.append(filters.prix_m2_max)
    if filters.surface_min is not None:
        clauses.append(f"{prefix}surface >= ?")
        params.append(filters.surface_min)
    if filters.surface_max is not None:
        clauses.append(f"{prefix}surface <= ?")
        params.append(filters.surface_max)
    if filters.nb_dpe_min:
        clauses.append(f"{prefix}nb_dpe >= ?")
        params.append(filters.nb_dpe_min)

    if not clauses:
        return WhereClause("", ())
    return WhereClause("WHERE " + " AND ".join(clauses), tuple(params))

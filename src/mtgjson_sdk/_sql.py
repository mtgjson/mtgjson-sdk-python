"""SQL builder with parameterized query construction."""

from __future__ import annotations

from typing import Any


class SQLBuilder:
    """Builds parameterized SQL queries safely.

    All user-supplied values go through DuckDB's parameter binding ($1, $2, ...),
    never through string interpolation. Methods return ``self`` for chaining.

    Example::

        sql, params = (
            SQLBuilder("cards")
            .where_eq("setCode", "MH3")
            .where_like("name", "Lightning%")
            .order_by("name ASC")
            .limit(10)
            .build()
        )
    """

    def __init__(self, base_table: str) -> None:
        """Create a builder targeting the given table or view.

        Args:
            base_table: The DuckDB table/view name to query from.
        """
        self._select: list[str] = ["*"]
        self._distinct: bool = False
        self._from = base_table
        self._joins: list[str] = []
        self._where: list[str] = []
        self._params: list[Any] = []
        self._group_by: list[str] = []
        self._having: list[str] = []
        self._order_by: list[str] = []
        self._limit: int | None = None
        self._offset: int | None = None

    def select(self, *columns: str) -> SQLBuilder:
        """Set the columns to select (replaces the default ``*``).

        Args:
            *columns: Column names or expressions.
        """
        self._select = list(columns)
        return self

    def distinct(self) -> SQLBuilder:
        """Add DISTINCT to the SELECT clause."""
        self._distinct = True
        return self

    def join(self, clause: str) -> SQLBuilder:
        """Add a JOIN clause.

        Args:
            clause: Full JOIN clause (e.g. ``"JOIN sets s ON cards.setCode = s.code"``).
        """
        self._joins.append(clause)
        return self

    def where(self, condition: str, *params: Any) -> SQLBuilder:
        """Add a WHERE condition with positional params using $N placeholders.

        The caller provides conditions with $N placeholders relative to
        the current param count. This method remaps them automatically.

        Args:
            condition: SQL condition with ``$1``, ``$2``, ... placeholders.
            *params: Values bound to the placeholders.
        """
        offset = len(self._params)
        # Remap $1, $2, ... to $N+1, $N+2, ...
        remapped = condition
        for i in range(len(params), 0, -1):
            remapped = remapped.replace(f"${i}", f"${offset + i}")
        self._where.append(remapped)
        self._params.extend(params)
        return self

    def where_like(self, column: str, value: str) -> SQLBuilder:
        """Add a case-insensitive LIKE condition.

        Args:
            column: Column name to match against.
            value: LIKE pattern (use ``%`` for wildcards).

        Example::

            q.where_like("name", "Lightning%")
            # → WHERE LOWER(name) LIKE LOWER($1)
        """
        idx = len(self._params) + 1
        self._where.append(f"LOWER({column}) LIKE LOWER(${idx})")
        self._params.append(value)
        return self

    def where_in(self, column: str, values: list[Any]) -> SQLBuilder:
        """Add an IN condition with parameterized values.

        Args:
            column: Column name.
            values: List of values for the IN clause. Empty list produces ``FALSE``.

        Example::

            q.where_in("uuid", ["abc", "def"])
            # → WHERE uuid IN ($1, $2)
        """
        if not values:
            self._where.append("FALSE")
            return self
        placeholders = []
        for v in values:
            idx = len(self._params) + 1
            placeholders.append(f"${idx}")
            self._params.append(v)
        self._where.append(f"{column} IN ({', '.join(placeholders)})")
        return self

    def where_eq(self, column: str, value: Any) -> SQLBuilder:
        """Add an equality condition.

        Args:
            column: Column name.
            value: Value to match.
        """
        idx = len(self._params) + 1
        self._where.append(f"{column} = ${idx}")
        self._params.append(value)
        return self

    def where_gte(self, column: str, value: Any) -> SQLBuilder:
        """Add a greater-than-or-equal condition.

        Args:
            column: Column name.
            value: Minimum value (inclusive).
        """
        idx = len(self._params) + 1
        self._where.append(f"{column} >= ${idx}")
        self._params.append(value)
        return self

    def where_lte(self, column: str, value: Any) -> SQLBuilder:
        """Add a less-than-or-equal condition.

        Args:
            column: Column name.
            value: Maximum value (inclusive).
        """
        idx = len(self._params) + 1
        self._where.append(f"{column} <= ${idx}")
        self._params.append(value)
        return self

    def where_regex(self, column: str, pattern: str) -> SQLBuilder:
        """Add a regex match condition (DuckDB ``regexp_matches``).

        Args:
            column: Column name.
            pattern: Regular expression pattern.
        """
        idx = len(self._params) + 1
        self._where.append(f"regexp_matches({column}, ${idx})")
        self._params.append(pattern)
        return self

    def where_fuzzy(
        self, column: str, value: str, *, threshold: float = 0.8
    ) -> SQLBuilder:
        """Add a fuzzy string match condition (Jaro-Winkler similarity).

        Matches rows where ``jaro_winkler_similarity(column, value) > threshold``.
        Useful for typo-tolerant search (e.g. "Ligtning Bolt" -> "Lightning Bolt").
        Caller should add ORDER BY similarity DESC for best results.

        Args:
            column: Column name to compare.
            value: Target string to match against.
            threshold: Minimum similarity score (0.0-1.0, default 0.8).

        Raises:
            ValueError: If threshold is not between 0 and 1.
        """
        if not isinstance(threshold, (int, float)) or not (0 <= threshold <= 1):
            raise ValueError(
                f"threshold must be a number between 0 and 1, got {threshold!r}"
            )
        idx = len(self._params) + 1
        self._where.append(f"jaro_winkler_similarity({column}, ${idx}) > {threshold}")
        self._params.append(value)
        return self

    def where_or(self, *conditions: tuple[str, Any]) -> SQLBuilder:
        """Add OR-combined conditions.

        Each condition is ``(sql_fragment, param_value)`` where the fragment
        uses ``$1`` as a placeholder (remapped automatically).

        Args:
            *conditions: Tuples of ``(condition_sql, param_value)``.

        Example::

            q.where_or(("name = $1", "Bolt"), ("name = $1", "Counter"))
            # → WHERE (name = $3 OR name = $4)
        """
        if not conditions:
            return self
        or_parts = []
        for cond, param in conditions:
            idx = len(self._params) + 1
            remapped = cond.replace("$1", f"${idx}")
            or_parts.append(remapped)
            self._params.append(param)
        self._where.append(f"({' OR '.join(or_parts)})")
        return self

    def group_by(self, *columns: str) -> SQLBuilder:
        """Add GROUP BY columns.

        Args:
            *columns: Column names or expressions to group by.
        """
        self._group_by.extend(columns)
        return self

    def having(self, condition: str, *params: Any) -> SQLBuilder:
        """Add a HAVING condition (works like where but for aggregates).

        Args:
            condition: SQL condition with ``$N`` placeholders.
            *params: Values bound to the placeholders.
        """
        offset = len(self._params)
        remapped = condition
        for i in range(len(params), 0, -1):
            remapped = remapped.replace(f"${i}", f"${offset + i}")
        self._having.append(remapped)
        self._params.extend(params)
        return self

    def order_by(self, *clauses: str) -> SQLBuilder:
        """Add ORDER BY clauses.

        Args:
            *clauses: Order clauses (e.g. ``"name ASC"``, ``"price DESC"``).
        """
        self._order_by.extend(clauses)
        return self

    def limit(self, n: int) -> SQLBuilder:
        """Set the maximum number of rows to return.

        Args:
            n: Non-negative integer limit.

        Raises:
            TypeError: If *n* is not a non-negative integer.
        """
        if not isinstance(n, int) or n < 0:
            raise TypeError(f"limit must be a non-negative integer, got {n!r}")
        self._limit = n
        return self

    def offset(self, n: int) -> SQLBuilder:
        """Set the number of rows to skip before returning results.

        Args:
            n: Non-negative integer offset.

        Raises:
            TypeError: If *n* is not a non-negative integer.
        """
        if not isinstance(n, int) or n < 0:
            raise TypeError(f"offset must be a non-negative integer, got {n!r}")
        self._offset = n
        return self

    def build(self) -> tuple[str, list[Any]]:
        """Build the final SQL string and parameter list.

        Returns:
            Tuple of ``(sql_string, params_list)`` ready for
            ``Connection.execute()``.
        """
        distinct = "DISTINCT " if self._distinct else ""
        parts = [f"SELECT {distinct}{', '.join(self._select)}", f"FROM {self._from}"]

        for join in self._joins:
            parts.append(join)

        if self._where:
            parts.append("WHERE " + " AND ".join(self._where))

        if self._group_by:
            parts.append("GROUP BY " + ", ".join(self._group_by))

        if self._having:
            parts.append("HAVING " + " AND ".join(self._having))

        if self._order_by:
            parts.append("ORDER BY " + ", ".join(self._order_by))

        if self._limit is not None:
            parts.append(f"LIMIT {self._limit}")

        if self._offset is not None:
            parts.append(f"OFFSET {self._offset}")

        return "\n".join(parts), self._params

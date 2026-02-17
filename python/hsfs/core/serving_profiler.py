#
#   Copyright 2024 Hopsworks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
import math
from collections import defaultdict


# Module-level profiling state.
# When None, profiling is off (zero overhead - just one `is not None` check).
# When a list, profiling is on and timing records are appended here.
_profiling_records: list[tuple[str, float]] | None = None


def _record(step: str, elapsed: float) -> None:
    """Append a profiling record. Only call when _profiling_records is not None."""
    _profiling_records.append((step, elapsed))


# Hierarchy used to render the tree view.
# Each key maps to the list of its children in display order.
_STEP_HIERARCHY = {
    "get_feature_vector total": [
        "validate_entry",
        "rest_fetch_single_total",
        "assemble_feature_vector",
        "format_return_type",
    ],
    "get_feature_vectors total": [
        "validate_entry_loop",
        "rest_fetch_batch_total",
        "assemble_feature_vector_loop",
        "format_return_type",
    ],
    "rest_fetch_single_total": [
        "payload_build",
        "api_get_single_raw",
        "response_decode_single",
    ],
    "rest_fetch_batch_total": [
        "payload_build",
        "api_get_batch_raw",
        "response_decode_batch",
    ],
    "api_get_single_raw": [
        "json_serialize",
        "http_request",
        "response_deserialize",
    ],
    "api_get_batch_raw": [
        "json_serialize",
        "http_request",
        "response_deserialize",
    ],
    "http_request": [
        "http_build_url",
        "http_prepare_request",
        "http_send",
    ],
    "http_send": [
        "http_ttfb",
        "http_body_download",
    ],
    "assemble_feature_vector": [
        "return_value_handlers",
        "transformations",
    ],
    "assemble_feature_vector_loop": [
        "return_value_handlers",
        "transformations",
    ],
    "transformations": [
        "on_demand_transformations",
        "model_dependent_transformations",
    ],
}


_ROOT_STEPS = {"get_feature_vector total", "get_feature_vectors total"}


class ServingProfiler:
    """Collects timing data and prints a formatted timing breakdown.

    Profiling is completely independent of log levels. No DEBUG logging is
    enabled and no log messages are emitted. Timing data is collected via a
    lightweight module-level list that instrumented code appends to directly.

    Parameters:
        warmup_pct: Fraction of requests considered warm-up (default 0.2).
            When > 0 and there are enough requests, ``print_summary`` will
            print an additional summary for the post-warm-up (steady-state)
            portion of the run.

    Usage::

        from hsfs.core.serving_profiler import ServingProfiler

        with ServingProfiler() as profiler:
            fv.get_feature_vector(entry={"pk": 1})
        # prints summary automatically

        # For multiple calls with averaging:
        with ServingProfiler() as profiler:
            for i in range(100):
                fv.get_feature_vector(entry={"pk": i})
            profiler.print_summary(aggregation="mean")
    """

    def __init__(self, warmup_pct: float = 0.2):
        self._warmup_pct = warmup_pct

    def __enter__(self):
        global _profiling_records
        _profiling_records = []
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.print_summary()
        global _profiling_records
        _profiling_records = None
        return False

    @property
    def records(self) -> list[tuple[str, float]]:
        if _profiling_records is None:
            return []
        return list(_profiling_records)

    def clear(self):
        if _profiling_records is not None:
            _profiling_records.clear()

    @staticmethod
    def _aggregate_records(
        records: list[tuple[str, float]], aggregation: str
    ) -> dict[str, float]:
        """Aggregate a list of ``(step, elapsed)`` records."""
        totals: dict[str, float] = defaultdict(float)
        counts: dict[str, int] = defaultdict(int)
        for step, elapsed in records:
            totals[step] += elapsed
            counts[step] += 1

        result: dict[str, float] = {}
        for step in totals:
            if aggregation == "mean" and counts[step] > 0:
                result[step] = totals[step] / counts[step]
            elif aggregation == "sum":
                result[step] = totals[step]
            else:
                result[step] = totals[step]
        return result

    @staticmethod
    def _collect_per_step_values(
        records: list[tuple[str, float]],
    ) -> dict[str, list[float]]:
        """Group records into per-step lists of elapsed values."""
        values: dict[str, list[float]] = defaultdict(list)
        for step, elapsed in records:
            values[step].append(elapsed)
        return values

    @staticmethod
    def _percentile(sorted_vals: list[float], pct: float) -> float:
        """Compute *pct*-th percentile from a **pre-sorted** list (linear interpolation)."""
        n = len(sorted_vals)
        if n == 1:
            return sorted_vals[0]
        k = (n - 1) * (pct / 100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_vals[int(k)]
        return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)

    @classmethod
    def _compute_percentile_data(
        cls,
        records: list[tuple[str, float]],
        percentiles: tuple[float, ...] = (50, 90, 95, 99),
    ) -> dict[str, dict[str, float]]:
        """Return ``{step: {"p50": v, "p90": v, …, "min": v, "max": v, "count": n}}``."""
        per_step = cls._collect_per_step_values(records)
        result: dict[str, dict[str, float]] = {}
        for step, vals in per_step.items():
            sv = sorted(vals)
            entry: dict[str, float] = {}
            for p in percentiles:
                entry[f"p{int(p)}"] = cls._percentile(sv, p)
            entry["min"] = sv[0]
            entry["max"] = sv[-1]
            entry["count"] = len(sv)
            result[step] = entry
        return result

    def _aggregate(self, aggregation: str) -> dict[str, float]:
        if _profiling_records is None:
            return {}
        return self._aggregate_records(list(_profiling_records), aggregation)

    def _split_records_by_warmup(
        self,
    ) -> tuple[list[tuple[str, float]], list[tuple[str, float]]]:
        """Split records into warm-up and post-warm-up portions.

        The split is based on request count.  A *request* is identified by the
        occurrence of a root step (``get_feature_vector total`` or
        ``get_feature_vectors total``).  Since root steps are recorded last
        (outer timing), we increment the request counter **after** the root
        record so that all sub-step records belong to the correct request.

        Returns:
            A ``(warmup_records, post_warmup_records)`` tuple.
        """
        if _profiling_records is None or self._warmup_pct <= 0:
            return [], list(_profiling_records or [])

        total_requests = sum(
            1 for step, _ in _profiling_records if step in _ROOT_STEPS
        )
        if total_requests < 2:
            return [], list(_profiling_records)

        warmup_count = max(1, math.ceil(total_requests * self._warmup_pct))

        warmup_records: list[tuple[str, float]] = []
        post_warmup_records: list[tuple[str, float]] = []
        request_num = 0

        for step, elapsed in _profiling_records:
            if request_num < warmup_count:
                warmup_records.append((step, elapsed))
            else:
                post_warmup_records.append((step, elapsed))
            if step in _ROOT_STEPS:
                request_num += 1

        return warmup_records, post_warmup_records

    @staticmethod
    def _build_tree_lines(data: dict[str, float]) -> list[tuple[str, str]]:
        """Build the list of ``(display_label, time_string)`` tree lines."""
        root_steps = [
            s
            for s in ["get_feature_vector total", "get_feature_vectors total"]
            if s in data
        ]
        if not root_steps:
            root_steps = list(data.keys())

        lines: list[tuple[str, str]] = []
        rendered: set[str] = set()

        def _render(step: str, prefix: str, is_last: bool):
            if step not in data:
                return
            rendered.add(step)
            connector = "\u2514\u2500 " if is_last else "\u251c\u2500 "
            display = f"{prefix}{connector}{step}"
            lines.append((display, f"{data[step]:.4f}"))

            children = _STEP_HIERARCHY.get(step, [])
            present_children = [c for c in children if c in data]
            child_prefix = prefix + ("   " if is_last else "\u2502  ")
            for i, child in enumerate(present_children):
                _render(child, child_prefix, i == len(present_children) - 1)

        for _i, root in enumerate(root_steps):
            rendered.add(root)
            lines.append((root, f"{data[root]:.4f}"))
            children = _STEP_HIERARCHY.get(root, [])
            present_children = [c for c in children if c in data]
            for j, child in enumerate(present_children):
                _render(child, "", j == len(present_children) - 1)

        for step in data:
            if step not in rendered:
                lines.append((step, f"{data[step]:.4f}"))

        return lines

    @staticmethod
    def _print_table(lines: list[tuple[str, str]], header: str) -> None:
        """Print a box-drawing table with the given *header* title."""
        if not lines:
            return

        step_width = max(len(l[0]) for l in lines) + 2
        time_width = max(len(l[1]) for l in lines) + 2
        step_width = max(step_width, len(" Step ") + 2)
        time_width = max(time_width, len(" Time (s) "))

        print(f"\n{header}")
        print("\u250c" + "\u2500" * step_width + "\u252c" + "\u2500" * time_width + "\u2510")
        print(
            "\u2502"
            + " Step".ljust(step_width)
            + "\u2502"
            + " Time (s)".ljust(time_width)
            + "\u2502"
        )
        print("\u251c" + "\u2500" * step_width + "\u253c" + "\u2500" * time_width + "\u2524")
        for step_display, time_display in lines:
            print(
                "\u2502 "
                + step_display.ljust(step_width - 1)
                + "\u2502"
                + time_display.rjust(time_width - 1)
                + " \u2502"
            )
        print("\u2514" + "\u2500" * step_width + "\u2534" + "\u2500" * time_width + "\u2518")

    @classmethod
    def _build_percentile_tree_lines(
        cls, pdata: dict[str, dict[str, float]]
    ) -> list[tuple[str, ...]]:
        """Build tree lines with percentile columns.

        Each element is ``(display_label, p50, p90, p95, p99, min, max, count)``.
        """
        _COLS = ("p50", "p90", "p95", "p99", "min", "max", "count")

        root_steps = [
            s
            for s in ["get_feature_vector total", "get_feature_vectors total"]
            if s in pdata
        ]
        if not root_steps:
            root_steps = list(pdata.keys())

        lines: list[tuple[str, ...]] = []
        rendered: set[str] = set()

        def _fmt(entry: dict[str, float]) -> tuple[str, ...]:
            return tuple(
                f"{entry[c]:.4f}" if c != "count" else str(int(entry[c]))
                for c in _COLS
            )

        def _render(step: str, prefix: str, is_last: bool):
            if step not in pdata:
                return
            rendered.add(step)
            connector = "\u2514\u2500 " if is_last else "\u251c\u2500 "
            display = f"{prefix}{connector}{step}"
            lines.append((display, *_fmt(pdata[step])))

            children = _STEP_HIERARCHY.get(step, [])
            present_children = [c for c in children if c in pdata]
            child_prefix = prefix + ("   " if is_last else "\u2502  ")
            for i, child in enumerate(present_children):
                _render(child, child_prefix, i == len(present_children) - 1)

        for root in root_steps:
            rendered.add(root)
            lines.append((root, *_fmt(pdata[root])))
            children = _STEP_HIERARCHY.get(root, [])
            present_children = [c for c in children if c in pdata]
            for j, child in enumerate(present_children):
                _render(child, "", j == len(present_children) - 1)

        for step in pdata:
            if step not in rendered:
                lines.append((step, *_fmt(pdata[step])))

        return lines

    @staticmethod
    def _print_percentile_table(
        lines: list[tuple[str, ...]], header: str
    ) -> None:
        """Print a multi-column percentile table with box-drawing borders."""
        if not lines:
            return

        col_headers = ("Step", "p50 (s)", "p90 (s)", "p95 (s)", "p99 (s)", "min (s)", "max (s)", "count")
        ncols = len(col_headers)

        col_widths: list[int] = []
        for ci in range(ncols):
            w = len(col_headers[ci]) + 2
            for row in lines:
                w = max(w, len(row[ci]) + 2)
            col_widths.append(w)

        def _sep(left: str, mid: str, right: str, fill: str = "\u2500") -> str:
            return left + mid.join(fill * w for w in col_widths) + right

        print(f"\n{header}")
        print(_sep("\u250c", "\u252c", "\u2510"))
        # Header row
        hdr = "\u2502"
        for ci, ch in enumerate(col_headers):
            if ci == 0:
                hdr += " " + ch.ljust(col_widths[ci] - 1)
            else:
                hdr += ch.rjust(col_widths[ci] - 1) + " "
            hdr += "\u2502"
        print(hdr)
        print(_sep("\u251c", "\u253c", "\u2524"))
        # Data rows
        for row in lines:
            line = "\u2502"
            for ci in range(ncols):
                cell = row[ci] if ci < len(row) else ""
                if ci == 0:
                    line += " " + cell.ljust(col_widths[ci] - 1)
                else:
                    line += cell.rjust(col_widths[ci] - 1) + " "
                line += "\u2502"
            print(line)
        print(_sep("\u2514", "\u2534", "\u2518"))

    def print_summary(self, aggregation: str = "sum"):
        """Print a formatted timing breakdown.

        When ``warmup_pct`` was set on the profiler (default **0.2**) and there
        are at least two requests, an additional *post-warm-up* summary is
        printed showing only the steady-state portion of the run (i.e. after
        the first 20 % of requests have been discarded).

        Percentile statistics (p50, p90, p95, p99, min, max) are printed
        whenever there are at least two samples for any step.

        Parameters:
            aggregation: How to aggregate across multiple calls.
                "sum" (default) shows total time, "mean" shows average per call.
        """
        if _profiling_records is None or not _profiling_records:
            print("No profiling data collected.")
            return

        all_records = list(_profiling_records)
        data = self._aggregate_records(all_records, aggregation)

        agg_label = f" (aggregation={aggregation})" if aggregation != "sum" else ""

        # Overall summary
        lines = self._build_tree_lines(data)
        self._print_table(
            lines, f"Serving Profiler Summary{agg_label}"
        )

        # Overall percentile statistics (only when there are repeated samples)
        pdata = self._compute_percentile_data(all_records)
        has_multiple = any(v["count"] >= 2 for v in pdata.values())
        if has_multiple:
            plines = self._build_percentile_tree_lines(pdata)
            self._print_percentile_table(
                plines, "Percentile Statistics (all requests)"
            )

        # Post-warm-up summary
        if self._warmup_pct > 0:
            warmup_records, post_warmup_records = self._split_records_by_warmup()
            if warmup_records and post_warmup_records:
                total_requests = sum(
                    1 for step, _ in all_records if step in _ROOT_STEPS
                )
                warmup_count = max(1, math.ceil(total_requests * self._warmup_pct))

                warmup_data = self._aggregate_records(warmup_records, "sum")
                post_data = self._aggregate_records(post_warmup_records, "sum")

                warmup_lines = self._build_tree_lines(warmup_data)
                post_lines = self._build_tree_lines(post_data)

                self._print_table(
                    warmup_lines,
                    f"Warm-up Summary (first {warmup_count} requests, "
                    f"{self._warmup_pct:.0%} of {total_requests})",
                )
                self._print_table(
                    post_lines,
                    f"Post-Warm-up Summary (remaining {total_requests - warmup_count} "
                    f"requests)",
                )

                # Post-warm-up percentile statistics
                post_pdata = self._compute_percentile_data(post_warmup_records)
                post_has_multiple = any(
                    v["count"] >= 2 for v in post_pdata.values()
                )
                if post_has_multiple:
                    post_plines = self._build_percentile_tree_lines(post_pdata)
                    self._print_percentile_table(
                        post_plines,
                        f"Percentile Statistics (post-warm-up, "
                        f"{total_requests - warmup_count} requests)",
                    )

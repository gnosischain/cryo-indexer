"""
Regression tests for StateManager.find_gaps.

Focus: a gap sitting between the last completed range and the end of the requested
window used to be invisible, so a scoped/chunked maintain silently left its final
batch unindexed while still reporting success.

These tests drive find_gaps against a fake ClickHouse client, so they need no server.
Run with:  python -m pytest tests/ -q
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core.state_manager import StateManager  # noqa: E402

BATCH = 100


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    """
    Answers the four query shapes find_gaps issues, driven by an in-memory list of
    (dataset, start_block, end_block, status) rows.
    """

    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def query(self, sql):
        self.queries.append(sql)
        squashed = " ".join(sql.split())

        # _get_highest_attempted_block
        if "MAX(end_block)" in squashed:
            candidates = [
                r[2] for r in self.rows
                if r[3] in ("completed", "failed", "processing", "pending")
            ]
            return FakeResult([(max(candidates),)] if candidates else [(None,)])

        # Step 7 per-gap re-check: exact (start, end) completed?
        if "COUNT(*)" in squashed and "start_block =" in squashed:
            start = int(_scalar(squashed, "start_block ="))
            end = int(_scalar(squashed, "end_block ="))
            n = sum(
                1 for r in self.rows
                if r[3] == "completed" and r[1] == start and r[2] == end
            )
            return FakeResult([(n,)])

        # Step 4b in-flight probe
        if "status = 'processing'" in squashed and "LIMIT 1" in squashed:
            lower = int(_scalar(squashed, "end_block >"))
            upper = int(_scalar(squashed, "start_block <"))
            hits = sorted(
                r[1] for r in self.rows
                if r[3] == "processing" and r[2] > lower and r[1] < upper
            )
            return FakeResult([(hits[0],)] if hits else [])

        # Step 3 completed ranges / Step 5 failed ranges
        status = "completed" if "status = 'completed'" in squashed else "failed"
        lo = int(_scalar(squashed, "start_block >="))
        hi = int(_scalar(squashed, "end_block <="))
        picked = sorted(
            (r[1], r[2]) for r in self.rows
            if r[3] == status and r[1] >= lo and r[2] <= hi
        )
        return FakeResult(picked)


def _scalar(squashed, prefix):
    """Pull the integer immediately following `prefix` out of a squashed SQL string."""
    tail = squashed.split(prefix, 1)[1].strip()
    return tail.split()[0]


class FakeDB:
    def __init__(self, rows):
        self.database = "testdb"
        self.client = FakeClient(rows)

    def _connect(self):
        return self.client


def make_manager(rows):
    return StateManager(FakeDB(rows))


def completed_run(start, end, dataset="transactions"):
    """Contiguous completed batches covering [start, end)."""
    return [
        (dataset, b, b + BATCH, "completed")
        for b in range(start, end, BATCH)
    ]


def test_gap_in_the_middle_is_found():
    """Baseline behaviour: a hole between two completed ranges is still detected."""
    rows = completed_run(1000, 1300) + completed_run(1400, 1700)
    gaps = make_manager(rows).find_gaps("transactions", 1000, 1700)
    assert gaps == [(1300, 1400)]


# Work attempted well beyond the requested window. Required for any tail-gap test:
# effective_end is clamped to the highest *attempted* block, so without evidence that
# indexing went past the window there is no tail to detect. Mirrors production, where
# the continuous indexer had carried `transactions` to ~72.98M while a maintain chunk
# ended at 67M.
BEYOND = [("transactions", 5000, 5100, "completed")]


def test_gap_at_tail_of_window_is_found():
    """
    The regression. Window ends at 1700 but completed data stops at 1600, so the final
    batch is missing. Previously returned [] because the loop only emitted gaps
    *between* completed ranges.
    """
    rows = completed_run(1000, 1600) + BEYOND
    gaps = make_manager(rows).find_gaps("transactions", 1000, 1700)
    assert gaps == [(1600, 1700)]


def test_tail_already_completed_yields_no_gap():
    """A fully covered window must stay silent -- no spurious trailing gap."""
    rows = completed_run(1000, 1700) + BEYOND
    gaps = make_manager(rows).find_gaps("transactions", 1000, 1700)
    assert gaps == []


def test_trailing_gap_smaller_than_one_batch_is_skipped():
    """
    Sub-batch remainder is below the 100-block floor in step 7, so it is dropped.
    Completed stops at 1650; window asks to 1700 -> 50-block tail.
    """
    rows = (
        completed_run(1000, 1600)
        + [("transactions", 1600, 1650, "completed")]
        + BEYOND
    )
    gaps = make_manager(rows).find_gaps("transactions", 1000, 1700)
    assert gaps == []


def test_tail_gap_invisible_when_nothing_attempted_beyond_window():
    """
    Complement to the above: with no evidence of work past the window, the clamp to
    highest_attempted means the tail is out of scope by design and must stay silent.
    Guards the pre-existing "only look for gaps WITHIN the attempted range" behaviour.
    """
    rows = completed_run(1000, 1600)
    gaps = make_manager(rows).find_gaps("transactions", 1000, 1700)
    assert gaps == []


def test_trailing_gap_clamped_below_in_flight_processing_range():
    """
    auto-maintain runs alongside the continuous indexer and callers DELETE before
    re-extracting, so a range still 'processing' at the tip must not be handed back.
    Completed to 1500; 1600-1700 is in flight -> only 1500-1600 is reclaimable.
    """
    rows = completed_run(1000, 1500) + [("transactions", 1600, 1700, "processing")]
    gaps = make_manager(rows).find_gaps("transactions", 1000, 1700)
    assert gaps == [(1500, 1600)]


def test_no_trailing_gap_when_only_the_tip_is_in_flight():
    """
    Nothing to reclaim: completed right up to the in-flight range. Emitting a gap here
    would delete rows the continuous indexer is actively writing.
    """
    rows = completed_run(1000, 1600) + [("transactions", 1600, 1700, "processing")]
    gaps = make_manager(rows).find_gaps("transactions", 1000, 1700)
    assert gaps == []


def test_trailing_gap_never_exceeds_highest_attempted():
    """
    Pre-existing guarantee ("only look for gaps WITHIN the attempted range") must hold:
    asking past the end of attempted work must not invent a gap out there.
    """
    rows = completed_run(1000, 1700)
    gaps = make_manager(rows).find_gaps("transactions", 1000, 99999)
    assert gaps == []


def test_failed_tail_range_still_reported():
    """A tail batch explicitly marked failed is reported via step 5, not duplicated."""
    rows = completed_run(1000, 1600) + [("transactions", 1600, 1700, "failed")]
    gaps = make_manager(rows).find_gaps("transactions", 1000, 1700)
    assert gaps == [(1600, 1700)]


def test_chunk_boundary_scenario_from_production():
    """
    The concrete case seen on celo_execution: maintain over 61000000-67000000 reported
    Fixed: N / Failed: 0 yet left 66999900-67000000 missing, and the next chunk starting
    at 67000000 could not see it either.
    """
    rows = completed_run(66999600, 66999900) + [
        ("transactions", 67000000, 67000100, "completed"),
    ]
    gaps = make_manager(rows).find_gaps("transactions", 61000000, 67000000)
    assert (66999900, 67000000) in gaps


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

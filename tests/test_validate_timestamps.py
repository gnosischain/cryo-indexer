"""
Regression tests for StateManager.validate_timestamps.

Focus: on ClickHouse Cloud (SharedMergeTree) a SELECT issued right after the INSERT
can still see none of the rows. The validation used to take that at face value and
declare the range bad, which made IndexerWorker.process_range bail out before Step 2 --
leaving blocks=completed and NO indexing_state row at all for transactions/logs.

The fix must keep the invariant intact: genuinely bad timestamps still fail, and fail
without burning retries.

These tests drive validate_timestamps against a fake ClickHouse client, so they need no
server. Run with:  python -m pytest tests/ -q
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.core.state_manager import StateManager  # noqa: E402

START, END = 73001500, 73001600
EXPECTED = END - START


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    """
    Answers the two count shapes validate_timestamps issues.

    visibility is a list, one entry per attempt-pair, of (present, valid) counts;
    the last entry is reused once exhausted. That models a replica catching up.
    """

    def __init__(self, visibility, sequential_consistency=True):
        self.visibility = visibility
        self.sequential_consistency = sequential_consistency
        self.queries = []
        self.settings_seen = []
        self._attempt = 0

    def query(self, sql, settings=None):
        self.queries.append(" ".join(sql.split()))
        self.settings_seen.append(settings)

        if settings and 'select_sequential_consistency' in settings and not self.sequential_consistency:
            raise RuntimeError("Unknown setting select_sequential_consistency")

        squashed = self.queries[-1]

        if "COUNT(DISTINCT block_number)" not in squashed:
            return FakeResult([])  # the debug sample query

        # A "valid" count carries the timestamp predicates; the visibility probe does not.
        # The probe closes out an attempt, so the entry only advances after it.
        present, valid = self.visibility[min(self._attempt, len(self.visibility) - 1)]
        if "timestamp IS NOT NULL" in squashed:
            return FakeResult([(valid,)])
        self._attempt += 1
        return FakeResult([(present,)])


class FakeDB:
    def __init__(self, client):
        self.database = "celo_execution"
        self.client = client

    def _connect(self):
        return self.client


def make_manager(visibility, sequential_consistency=True):
    client = FakeClient(visibility, sequential_consistency)
    manager = StateManager(FakeDB(client))
    # Keep the suite fast: the production backoff would sleep ~7.5s across five attempts.
    manager.TIMESTAMP_VALIDATION_BACKOFF_SECONDS = 0
    return manager, client


def test_visible_immediately_passes():
    manager, client = make_manager([(EXPECTED, EXPECTED)])
    check = manager.validate_timestamps(START, END)
    assert check.ok and check.reason == 'ok'
    # One query is enough when the data is already there.
    assert len(client.queries) == 1


def test_read_after_write_lag_is_retried_not_failed():
    """
    The regression: the first read sees nothing (rows inserted ~1s ago), the next sees
    all 100. Previously this returned False and the range lost transactions/logs.
    """
    manager, _ = make_manager([(0, 0), (EXPECTED, EXPECTED)])
    check = manager.validate_timestamps(START, END)
    assert check.ok and check.reason == 'ok'


def test_partial_visibility_is_retried():
    """Rows landing gradually is still lag, not corruption."""
    manager, _ = make_manager([(40, 40), (90, 90), (EXPECTED, EXPECTED)])
    assert manager.validate_timestamps(START, END).ok


def test_bad_timestamps_still_fail():
    """
    The invariant that must not weaken: all 100 blocks readable, only 98 with a usable
    timestamp -> dependent datasets must not be indexed against them.
    """
    manager, _ = make_manager([(EXPECTED, 98)])
    check = manager.validate_timestamps(START, END)
    assert not check.ok
    assert check.reason == 'invalid'
    assert (check.valid, check.present) == (98, EXPECTED)


def test_bad_timestamps_fail_without_retrying():
    """Visible-but-wrong cannot change with time, so it must not burn the retry budget."""
    manager, client = make_manager([(EXPECTED, 0)])
    manager.validate_timestamps(START, END)
    counts = [q for q in client.queries if "COUNT(DISTINCT block_number)" in q]
    assert len(counts) == 2  # one valid-count, one visibility probe, then stop


def test_permanently_invisible_reports_not_visible():
    """Exhausted retries are reported as a visibility failure, distinct from 'invalid'."""
    manager, _ = make_manager([(0, 0)])
    check = manager.validate_timestamps(START, END)
    assert not check.ok
    assert check.reason == 'not_visible'


def test_uses_sequential_consistency():
    """The count must be able to observe the writer's own insert."""
    manager, client = make_manager([(EXPECTED, EXPECTED)])
    manager.validate_timestamps(START, END)
    assert client.settings_seen[0] == {'select_sequential_consistency': 1}


def test_falls_back_when_setting_rejected():
    """A server that does not accept the setting must not break validation."""
    manager, client = make_manager([(EXPECTED, EXPECTED)], sequential_consistency=False)
    check = manager.validate_timestamps(START, END)
    assert check.ok
    assert client.settings_seen[-1] is None
    assert manager._sequential_consistency_supported is False


def test_empty_range_is_trivially_ok():
    manager, client = make_manager([(0, 0)])
    assert manager.validate_timestamps(START, START).ok
    assert client.queries == []


def test_has_valid_timestamps_wrapper_still_returns_bool():
    manager, _ = make_manager([(0, 0), (EXPECTED, EXPECTED)])
    assert manager.has_valid_timestamps(START, END) is True

    manager, _ = make_manager([(EXPECTED, 0)])
    assert manager.has_valid_timestamps(START, END) is False


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-q"]))

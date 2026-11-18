"""The ingest path and the incremental cursor.

The walk is driven with a plain dict as dlt resource state: no network, no LLM. The fake
records every call, so a test can pin the ``fromdate`` the incremental listing sends and
the pages a cut run resumes at.
"""

import pytest
from fake_stack_overflow import FakeStackExchange

from cognee_connector_stack_overflow import source as so_module
from cognee_connector_stack_overflow.source import (
    ID_BATCH,
    PAGE_SIZE,
    StackOverflowAPIError,
    StackOverflowRateLimitedError,
    _iter_rows,
)
from cognee_connectors_core.dlt_conventions import NODE_SET_COLUMN

ROW_COLUMNS = {"id", "title", "content", "url", "_deleted", NODE_SET_COLUMN}
COUNT_KEYS = {"scanned", "skipped", "deleted", "failed", "requests"}


def _sync(
    fake: FakeStackExchange, state: dict, site: str = "stackoverflow", **kwargs
) -> tuple[list[dict], dict]:
    stats: dict[str, int] = {}
    rows = list(_iter_rows(fake, state, stats, site=site, **kwargs))
    return rows, stats


def _ids(rows: list[dict]) -> set[str]:
    return {row["id"] for row in rows}


def _tombstones(rows: list[dict]) -> set[str]:
    return {row["id"] for row in rows if row.get("_deleted")}


def _workspace() -> FakeStackExchange:
    fake = FakeStackExchange()
    fake.add_question(101, "How do I add memory to an agent?", tags=["cognee", "python"])
    fake.add_question(102, "What is the fastest Python JSON parser?", tags=["python"])
    fake.add_answer(1001, 101, accepted=True)
    fake.add_answer(1002, 101)
    fake.add_answer(1003, 102)
    return fake


def _params(fake: FakeStackExchange, path: str) -> list[dict]:
    return [params for called, params in fake.calls if called == path]


def test_first_sync_emits_every_question_and_answer_with_the_frozen_row_shape():
    rows, stats = _sync(_workspace(), {})

    assert _ids(rows) == {
        "question:101",
        "question:102",
        "answer:1001",
        "answer:1002",
        "answer:1003",
    }
    assert all(set(row) == ROW_COLUMNS for row in rows)
    assert all(row["_deleted"] is False and row["title"] and row["content"] for row in rows)
    urls = {row["id"]: row["url"] for row in rows}
    assert urls["question:101"] == "https://stackoverflow.com/questions/101"
    assert urls["answer:1001"] == "https://stackoverflow.com/a/1001"
    assert stats["failed"] == 0


def test_every_row_carries_the_node_set_of_its_site_and_kind():
    fake = FakeStackExchange(site="serverfault")
    fake.add_question(101, "Why is my RAID slow?", tags=["raid"])
    fake.add_answer(1001, 101)

    rows, _ = _sync(fake, {}, site="serverfault")

    assert {row["id"]: row[NODE_SET_COLUMN] for row in rows} == {
        "question:101": ["stack_overflow:serverfault:questions"],
        "answer:1001": ["stack_overflow:serverfault:answers"],
    }


def test_an_answer_names_its_question_by_the_questions_own_title():
    rows, _ = _sync(_workspace(), {})
    answer = next(row for row in rows if row["id"] == "answer:1001")

    assert answer["title"] == "How do I add memory to an agent?"
    assert answer["content"].splitlines()[0] == (
        'Answer by Ben Turing to "How do I add memory to an agent?".'
    )
    assert "Accepted: yes" in answer["content"].splitlines()


def test_a_question_opens_with_its_kind_and_carries_its_fields():
    rows, _ = _sync(_workspace(), {})
    question = next(row for row in rows if row["id"] == "question:101")

    assert question["content"].splitlines()[:6] == [
        'Stack Overflow question "How do I add memory to an agent?".',
        "Asked by: Ada Lovelace",
        "Tags: cognee, python",
        "Score: 1",
        "Answers: 2",
        "Link: https://stackoverflow.com/questions/101",
    ]
    assert "Question body." in question["content"]


# --- the incremental cursor ---


def test_the_first_run_omits_fromdate_and_lists_everything():
    fake = _workspace()

    _sync(fake, {})

    listings = [p for p in _params(fake, "/questions") if p.get("filter") == "withbody"]
    assert len(listings) == 1
    assert "fromdate" not in listings[0]


def test_the_second_run_lists_only_what_changed_since_the_cursor():
    fake, state = _workspace(), {}
    _sync(fake, state)

    rows, stats = _sync(fake, state)

    assert rows == []
    assert stats["skipped"] == stats["scanned"] == 2, (
        "only the question at the cursor and its answers are re-scanned"
    )
    assert state["cursor"] == max(q["last_activity_date"] for q in fake.questions.values())
    fromdates = [
        p["fromdate"]
        for p in _params(fake, "/questions")
        if p.get("filter") == "withbody" and "fromdate" in p
    ]
    assert fromdates, "the incremental listing passes the stored cursor as fromdate"
    assert all(f == state["cursor"] for f in fromdates)


def test_an_edit_re_emits_only_that_question_and_its_answers():
    fake, state = _workspace(), {}
    _sync(fake, state)

    fake.questions[102]["title"] = "What is the fastest JSON parser today?"
    fake.questions[102]["last_activity_date"] = fake._next_activity()
    rows, _ = _sync(fake, state)

    assert _ids(rows) == {"question:102", "answer:1003"}


def test_a_new_answer_re_emits_the_question_and_the_answer():
    fake, state = _workspace(), {}
    _sync(fake, state)

    fake.add_answer(1004, 102)
    fake.questions[102]["last_activity_date"] = fake._next_activity()
    rows, _ = _sync(fake, state)

    assert _ids(rows) == {"question:102", "answer:1004"}


def test_the_cursor_is_the_highest_activity_of_the_completed_listing():
    fake, state = _workspace(), {}
    _sync(fake, state)
    first = state["cursor"]

    fake.add_question(103, "Another one?", tags=["python"])
    rows, _ = _sync(fake, state)

    assert _ids(rows) == {"question:103"}
    assert state["cursor"] > first


def test_a_question_of_another_tag_is_never_listed():
    fake = _workspace()
    fake.add_question(103, "Off topic?", tags=["cooking"])

    rows, _ = _sync(fake, {}, tags=("python",))

    assert "question:103" not in _ids(rows)


# --- forget-on-delete ---


def test_a_deleted_question_is_tombstoned_with_its_answers():
    fake, state = _workspace(), {}
    _sync(fake, state)

    del fake.questions[102]
    rows, stats = _sync(fake, state)

    assert _tombstones(rows) == {"question:102", "answer:1003"}
    assert stats["deleted"] == 2
    assert "102" not in state["question_answers"]


def test_a_question_that_lost_the_selected_tags_is_forgotten():
    fake, state = _workspace(), {}
    fake.add_question(103, "Still on topic?", tags=["cognee"])
    _sync(fake, state, tags=("cognee",))

    # 101 keeps the other tags of its own edit, but no longer carries the synced one; the
    # sweep still sees 103, so the empty-sweep guard does not apply
    fake.questions[101]["tags"] = ["python"]
    fake.questions[101]["last_activity_date"] = fake._next_activity()
    rows, _ = _sync(fake, state, tags=("cognee",))

    assert _tombstones(rows) == {"question:101", "answer:1001", "answer:1002"}


def test_a_deleted_answer_is_tombstoned_when_its_question_is_relisted():
    fake, state = _workspace(), {}
    _sync(fake, state)

    del fake.answers[1002]
    fake.questions[101]["answer_count"] = 1
    fake.questions[101]["last_activity_date"] = fake._next_activity()
    rows, _ = _sync(fake, state)

    assert _tombstones(rows) == {"answer:1002"}


def test_a_question_missing_from_the_sweep_but_still_there_is_kept():
    """The sweep is a second listing minutes after the incremental one; a question that
    appears in neither is deleted, one that only the sweep missed transiently is kept."""
    fake, state = _workspace(), {}
    _sync(fake, state)

    # the sweep pages over a stale copy: 102 is not returned there, but the batch check
    # still finds it, so it stays
    real_search = fake._search

    def stale_search(params):
        items = real_search(params)
        if "filter" not in params:  # the sweep, unlike the incremental listing
            items = [q for q in items if q["question_id"] != 102]
        return items

    fake._search = stale_search
    rows, _ = _sync(fake, state)

    assert rows == []
    assert "question:102" in state["fingerprints"]


def test_a_failed_deletion_check_keeps_the_question():
    fake, state = _workspace(), {}
    _sync(fake, state)

    del fake.questions[102]
    fake.fail_paths["/questions/102"] = StackOverflowAPIError(
        "Stack Exchange request failed: HTTP 500"
    )
    rows, stats = _sync(fake, state)

    assert rows == []
    assert stats["failed"] == 1
    assert stats["failed_deletion_check"] == 1
    assert "question:102" in state["fingerprints"]


def test_a_question_id_that_is_not_numeric_is_never_requested():
    fake, state = _workspace(), {}
    _sync(fake, state)
    state["fingerprints"]["question:not-an-id"] = "x"

    rows, stats = _sync(fake, state)

    assert rows == []
    assert stats["failed_deletion_check"] == 1
    assert "question:not-an-id" in state["fingerprints"]
    assert fake.count("/questions/not-an-id") == 0


def test_deselecting_answers_forgets_them_without_a_request():
    fake, state = _workspace(), {}
    _sync(fake, state)

    rows, stats = _sync(fake, state, include=("questions",))

    assert _tombstones(rows) == {"answer:1001", "answer:1002", "answer:1003"}
    assert stats["deleted"] == 3
    assert state["question_answers"] == {}
    assert fake.count("/questions/101/answers") == 0


# --- an empty sweep is a broken filter, not mass deletion ---


def test_an_empty_sweep_forgets_nothing_and_reports_failed_sweep():
    fake, state = _workspace(), {}
    _sync(fake, state)

    fake.questions.clear()
    rows, stats = _sync(fake, state)

    assert rows == []
    assert stats["failed"] == 1
    assert stats["failed_sweep"] == 1
    assert len(state["fingerprints"]) == 5, "nothing was forgotten"
    assert state["sweep"] == {}, "the sweep is retried next run"

    rows, stats = _sync(fake, state)
    assert rows == [] and stats["failed_sweep"] == 1


# --- budget, quota, rate limit and resuming ---


def test_a_budget_cut_during_the_listing_resumes_at_the_same_page():
    fake, state = FakeStackExchange(), {}
    for n in range(PAGE_SIZE * 2 + 10):
        fake.add_question(1000 + n, f"Question {n}?", tags=["python"])
    # the listing's page 1 passes, the page's answers fetch is over budget: the page is
    # not consumed and the run starts there again
    rows, stats = _sync(fake, state, tags=("python",), max_requests=1)

    assert rows == []
    assert stats["failed_budget"] == 1
    assert state["walk"]["page"] == 1

    rows, stats = _sync(fake, state, tags=("python",))
    assert stats["failed"] == 0
    assert _tombstones(rows) == set()
    assert _ids(rows) == {f"question:{1000 + n}" for n in range(PAGE_SIZE * 2 + 10)}
    assert state["cursor"] > 0


def test_a_budget_cut_during_the_sweep_resumes_it_and_detects_deletes():
    fake, state = _workspace(), {}
    _sync(fake, state)
    del fake.questions[102]

    _, stats = _sync(fake, state, max_requests=1)  # the listing, then the sweep is cut
    assert stats["failed_budget"] == 1

    rows, stats = _sync(fake, state)
    assert stats["failed"] == 0
    assert _tombstones(rows) == {"question:102", "answer:1003"}


def test_low_remaining_quota_stops_the_run_before_the_next_request():
    fake, state = _workspace(), {}
    _sync(fake, state)

    fake.quota_remaining = 5
    rows, stats = _sync(fake, state)

    assert rows == []
    assert stats["failed_quota"] == 1
    assert stats["failed"] == 1


def test_a_throttled_run_stops_cleanly_and_resumes():
    fake, state = _workspace(), {}
    _sync(fake, state)
    del fake.questions[102]

    fake.fail_paths["/questions"] = StackOverflowRateLimitedError(
        "Stack Exchange throttled the request (HTTP 429)"
    )
    rows, stats = _sync(fake, state)
    assert rows == []
    assert stats["failed_rate_limit"] == 1

    del fake.fail_paths["/questions"]
    rows, stats = _sync(fake, state)
    assert stats["failed"] == 0
    assert _tombstones(rows) == {"question:102", "answer:1003"}


def test_every_run_reports_every_count_key_however_it_ends():
    _, stats = _sync(_workspace(), {})
    assert COUNT_KEYS <= stats.keys()


def test_check_active_runs_before_every_request_and_can_stop_the_sync():
    fake, checks = _workspace(), []

    def check_active():
        checks.append(len(fake.calls))
        if len(checks) > 2:
            raise PermissionError("access revoked")

    with pytest.raises(PermissionError):
        _sync(fake, {}, check_active=check_active)

    assert checks == [0, 1, 2]
    assert len(fake.calls) == 2


def test_a_changed_node_set_re_emits_what_the_cursor_relists(monkeypatch):
    """The node set is part of the fingerprint: what the incremental listing re-fetches
    (the question sitting at the cursor, inclusive, and its answers) must be replaced by
    rows carrying the new set, not skipped because its text did not change."""
    fake, state = _workspace(), {}
    _sync(fake, state)

    monkeypatch.setattr(so_module, "_node_set", lambda site, kind: [f"stack_overflow:{kind}:v2"])
    rows, _ = _sync(fake, state)

    assert {row["id"]: row[NODE_SET_COLUMN] for row in rows} == {
        "question:102": ["stack_overflow:questions:v2"],
        "answer:1003": ["stack_overflow:answers:v2"],
    }


def test_a_tag_change_restarts_the_listing():
    fake, state = _workspace(), {}
    _sync(fake, state, tags=("python",))

    rows, stats = _sync(fake, state, tags=("cognee",))

    assert stats["failed"] == 0
    assert _tombstones(rows) == {"question:102", "answer:1003"}

"""Stack Overflow connector for cognee: a ``dlt`` source that turns tagged Q&A into memory.

One source is one (site, tag selection) pair on the Stack Exchange API. It yields the
selected tags' questions and their answers as flat document rows and is meant to be handed
directly to :func:`cognee.remember`::

    import cognee
    from cognee_connector_stack_overflow import stack_overflow_source

    await cognee.remember(
        stack_overflow_source(tags=["cognee"], resource_name="stack_overflow_cognee"),
        dataset_name="stack_overflow",
        primary_key="id",
        write_disposition="merge",   # REQUIRED, the add pipeline defaults to "replace"
        max_rows_per_table=0,
    )

Design
------
* **Rows** are flat ``{id, title, content, url, _deleted, cognee_node_set}``. Ids are
  prefixed by kind (``question:<id>``, ``answer:<id>``). Every column this source emits
  feeds the stored document identity, so the rendering is frozen and deterministic: lists
  are sorted and nothing volatile (view counts, live scores that change with every vote)
  is rendered beyond the score itself.
* **Node set** ``stack_overflow:<site>:<kind>`` (``questions``, ``answers``) scopes a
  ``recall`` to Stack Overflow, to one site of the Stack Exchange network, or to one kind.
* **Incremental** by activity cursor. The Stack Exchange API filters by ``fromdate`` on a
  record's last activity, so each run lists only questions active since the last run and a
  row is emitted only when its rendered text changed (a fingerprint kept in dlt resource
  state). The cursor is the highest ``last_activity_date`` of a completed run.
* **Forget-on-delete.** The Stack Exchange listings never contain deleted records, so the
  run that completed its incremental listing sweeps the full id set of the tag selection
  and batch-reads the missing ids (up to 100 per request). A question that no longer
  exists, or that lost the selected tags, is forgotten together with its answers. An
  answer that vanished from its question's answer list is forgotten when the question is
  re-listed.
* **Quota.** The API quota is per-day and small (300 calls without a key, 10,000 with
  one), and every answer carries ``quota_remaining`` and an optional ``backoff`` the
  client must honor. A run stops cleanly on a spent request budget, less than 10 calls
  left in the daily quota, or a throttle violation, keeps the state it reached and reports
  the reason in ``cognee_sync_stats``; the next run resumes. It never waits out a rate
  limit: ``dlt`` rolls the state of a failed run back, so a selection too big for one
  window would never finish.
* **An empty sweep means a broken filter, not mass deletion.** A tag selection that
  matches nothing returns an empty listing, and every synced record would look deleted.
  When a completed sweep returns zero questions while records are synced, no deletion is
  acted on and the run reports ``failed_sweep`` instead.
* **Key** is a plain Stack Exchange API ``key`` (query parameter). It lives in the client
  object only; errors carry statuses and endpoints, never the key. A redirect is an
  error, not followed: ``requests`` would send the key to whatever host it names.
"""

import hashlib
import json
import logging
import re
import time
from collections.abc import Callable, Iterable, Iterator
from typing import Any

from cognee_connectors_core import dlt_conventions as dlt_utils
from cognee_connectors_core.dlt_conventions import DOCUMENT_SOURCE_ATTR, NODE_SET_COLUMN

logger = logging.getLogger(__name__)

BASE_URL = "https://api.stackexchange.com/2.3"
# The Stack Exchange maximum pagesize.
PAGE_SIZE = 100
# ids per read: the API accepts up to 100 semicolon-separated ids per request.
ID_BATCH = 100
# A run over one tag selection; the daily quota stops bigger ones first.
DEFAULT_MAX_REQUESTS = 200
# Calls left untouched in the daily quota, for the key's other users.
_QUOTA_RESERVE = 10
# (connect, read) seconds; our own choice, Stack Exchange documents no expected latency.
_TIMEOUT = (10, 30)
# Attempts per request on 5xx or network errors, backing off 1s then 2s.
_RETRIES = 3
# The built-in filter that renders body_markdown on questions and answers.
WITHBODY_FILTER = "withbody"
# The error_id Stack Exchange uses for a throttle violation.
_THROTTLE_ERROR_ID = 502
# A record id goes into the URL of its own read, so only digits may get through: a ";",
# "/" or ".." in a batch path would read another endpoint.
_RECORD_ID = re.compile(r"[0-9]+")
# Stack Exchange sorts /questions by descending activity by default.
KINDS = ("questions", "answers")


class StackOverflowSourceError(RuntimeError):
    """Base class of the errors this source raises."""


class StackOverflowAPIError(StackOverflowSourceError):
    """Stack Exchange answered with an error, or could not be reached."""


class StackOverflowAuthError(StackOverflowSourceError):
    """Stack Exchange rejected the credentials (HTTP 401)."""


class StackOverflowRateLimitedError(StackOverflowSourceError):
    """Stack Exchange throttled the key (HTTP 429 or error_id 502)."""


# The count key of an error that ends a run by raising, matched with isinstance.
_RAISED_REASONS = {StackOverflowAuthError: "failed_auth"}


def _text(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(sorted(text for text in map(_text, value) if text))
    return "" if value is None else str(value).strip()


def _lines(fields: list[tuple[str, str]]) -> list[str]:
    return [f"{label}: {value}" for label, value in fields if value]


def _endpoint(path: str) -> str:
    """The path with a record id batch replaced, so error messages never carry one."""
    head, _, _tail = path.rpartition("/")
    if head == "/questions" and path not in ("/questions",):
        return f"{head}/{{ids}}"
    return path


class _Transient(Exception):
    """One failed attempt worth another: a network error (``response`` None) or a 5xx answer."""

    def __init__(self, response: Any, reason: str):
        super().__init__(reason)
        self.response = response
        self.reason = reason


class StackOverflowClient:
    """Minimal synchronous client for the Stack Exchange API.

    Every answer carries ``quota_remaining`` and may carry ``backoff`` (seconds the API
    asks the client to wait before the next call); after each answer, an error included,
    the client sleeps the requested backoff before the next request. Tests and hosts can
    pass any object with the same ``request`` method, a ``quota_remaining`` attribute and
    a ``rate_limit`` attribute instead: ``request`` takes a ``check_active`` keyword and
    must call it before each request attempt, retries and waits included.
    """

    def __init__(self, api_key: str | None = None, *, sleep: Callable[[float], None] = time.sleep):
        key = (api_key or "").strip()
        # This check is what keeps the key out of an exception: requests names the value of
        # a query parameter it rejects, and every key it would reject is refused here.
        if key and (not key.isascii() or not key.isprintable() or " " in key):
            raise ValueError("The Stack Exchange API key has an invalid format")
        self._api_key = key
        self._sleep = sleep
        self.quota_remaining: int | None = None
        # Seconds the last answer asked for before the next call; 0.5 is the floor the
        # API docs name for non-intensive apps, and what a missing backoff resets to when
        # the answer carried one of 0.
        self._backoff = 0.0

    def __repr__(self) -> str:
        return "StackOverflowClient(api_key=<redacted>)"

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        check_active: Callable[[], None] | None = None,
    ) -> dict[str, Any]:
        import requests

        def attempt() -> Any:
            if self._backoff > 0:
                wait, self._backoff = self._backoff, 0.0
                self._sleep(wait)
            if check_active is not None:
                check_active()
            query = {"site": None, **(params or {})}
            if self._api_key:
                query["key"] = self._api_key
            try:
                response = requests.request(
                    method,
                    BASE_URL + path,
                    params={k: v for k, v in query.items() if v is not None},
                    timeout=_TIMEOUT,
                    allow_redirects=False,
                )
            except requests.RequestException as exc:
                raise _Transient(None, type(exc).__name__) from None
            if response.status_code >= 500:
                raise _Transient(response, f"HTTP {response.status_code}")
            return self._parse(response, path)

        try:
            return dlt_utils.with_retries(
                attempt,
                retry_on=(_Transient,),
                delay=lambda error, number: 2**number,
                attempts=_RETRIES,
                check_active=check_active,
                sleep=self._sleep,
            )
        except _Transient as error:
            # The last attempt failed the same way: a 5xx answer is read like any other
            # answer, a network error is reported by type. `from None` keeps the cause out
            # of this error; `__context__`, which dlt's trace file still reads, carries no
            # key because the key is a validated query parameter, not a header value.
            if error.response is None:
                raise StackOverflowAPIError(
                    f"Stack Exchange request failed: {error.reason}"
                ) from None
            return self._parse(error.response, path)

    def _parse(self, response: Any, path: str) -> dict[str, Any]:
        status = response.status_code
        if 300 <= status < 400:
            raise StackOverflowAPIError(
                f"Stack Exchange answered {_endpoint(path)} with a redirect (HTTP {status})"
            )
        if status == 401:
            raise StackOverflowAuthError("Stack Exchange rejected the credentials (HTTP 401)")
        if status == 429:
            raise StackOverflowRateLimitedError("Stack Exchange throttled the request (HTTP 429)")
        try:
            envelope = response.json()
        except ValueError:
            envelope = None
        if not isinstance(envelope, dict):
            raise StackOverflowAPIError(
                f"Stack Exchange sent an unreadable answer for {_endpoint(path)}"
            )
        self._read_backoff(envelope)
        if envelope.get("error_id") is not None:
            if envelope.get("error_id") == _THROTTLE_ERROR_ID:
                raise StackOverflowRateLimitedError(
                    f"Stack Exchange throttled {_endpoint(path)} (error_id {_THROTTLE_ERROR_ID})"
                )
            # The error message is not echoed: Stack Exchange phrases parameter errors with
            # the parameter name, and the query string is what carries the key.
            raise StackOverflowAPIError(
                f"Stack Exchange answered {_endpoint(path)} with error_id {envelope['error_id']}"
            )
        if status != 200:
            raise StackOverflowAPIError(f"Stack Exchange request failed: HTTP {status}")
        if not isinstance(envelope.get("items"), list):
            raise StackOverflowAPIError(
                f"Stack Exchange sent an unreadable answer for {_endpoint(path)}"
            )
        remaining = envelope.get("quota_remaining")
        if isinstance(remaining, int):
            self.quota_remaining = remaining
        return envelope

    def _read_backoff(self, envelope: dict[str, Any]) -> None:
        backoff = envelope.get("backoff")
        if isinstance(backoff, (int, float)) and backoff > 0:
            self._backoff = float(backoff)


def build_stack_overflow_service(api_key: str | None = None) -> StackOverflowClient:
    """Wrap a host-owned API key in a client, the way Apollo does."""
    return StackOverflowClient(api_key)


# ---------------------------------------------------------------------------
# Rendering: deterministic, first-party fields only, nothing volatile.
# ---------------------------------------------------------------------------
def _owner_name(record: dict[str, Any]) -> str:
    owner = record.get("owner")
    if isinstance(owner, dict):
        return _text(owner.get("display_name"))
    return ""


def _site_host(site: str) -> str:
    return f"https://{site}.com"


def render_question(question: dict[str, Any], site: str) -> dict[str, Any]:
    """A question's row. Each record is its own document; the answers name the question by
    the same title, so the graph joins a question to its answers."""
    question_id = question["question_id"]
    title = _text(question.get("title")) or "Untitled question"
    link = _text(question.get("link")) or f"{_site_host(site)}/questions/{question_id}"
    fields = [
        ("Asked by", _owner_name(question) or "an unknown user"),
        ("Tags", _text(question.get("tags"))),
        ("Score", _text(question.get("score"))),
        ("Answers", _text(question.get("answer_count"))),
        ("Link", link),
    ]
    body = _text(question.get("body_markdown"))
    content = "\n".join([f'Stack Overflow question "{title}".', *_lines(fields)])
    if body:
        content = f"{content}\n\n{body}"
    return {
        "id": f"question:{question_id}",
        "title": title,
        "content": content,
        "url": link,
        "_deleted": False,
    }


def render_answer(
    answer: dict[str, Any], question_title: str, question_id: int | str, site: str
) -> dict[str, Any]:
    answer_id = answer["answer_id"]
    title = _text(question_title) or "Untitled question"
    fields = [
        ("Answered by", _owner_name(answer) or "an unknown user"),
        ("Score", _text(answer.get("score"))),
        ("Accepted", "yes" if answer.get("is_accepted") else ""),
        ("Link", f"{_site_host(site)}/a/{answer_id}"),
    ]
    body = _text(answer.get("body_markdown"))
    content = "\n".join(
        [f'Answer by {_owner_name(answer) or "an unknown user"} to "{title}".', *_lines(fields)]
    )
    if body:
        content = f"{content}\n\n{body}"
    return {
        "id": f"answer:{answer_id}",
        "title": title,
        "content": content,
        "url": f"{_site_host(site)}/a/{answer_id}",
        "_deleted": False,
    }


def _node_set(site: str, kind: str) -> list[str]:
    """The node set of a row: ``stack_overflow:<site>:<kind>``."""
    return [f"stack_overflow:{site}:{kind}"]


def _hash(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _valid_ids(ids: Iterable[Any]) -> list[str]:
    return [str(i) for i in ids if isinstance(i, (int, str)) and _RECORD_ID.fullmatch(str(i))]


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------
class _CutShortError(Exception):
    """Internal: stop the run here, keeping the state reached so far."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class _Walker:
    """One run over a (site, tag selection): the changed listing, the answer fetch per
    page, the id sweep and the deletion checks, writing progress into ``state``."""

    def __init__(
        self,
        client: Any,
        state: dict,
        stats: dict[str, int],
        *,
        site: str,
        tags: tuple[str, ...],
        include: tuple[str, ...],
        max_requests: int,
        check_active: Callable[[], None] | None,
    ):
        self.client = client
        self.state = state
        self.stats = stats
        for key in ("scanned", "skipped", "deleted", "failed", "requests"):
            stats.setdefault(key, 0)
        self.site = site
        self.tags = tags
        self.include = include
        self.max_requests = max_requests
        self.check_active = check_active
        self.emitted = 0
        self.fingerprints: dict[str, str] = state.setdefault("fingerprints", {})
        self.question_answers: dict[str, list[str]] = state.setdefault("question_answers", {})
        self.seen: list[str] = state.setdefault("seen", [])
        # Questions seen this run (incremental listing and sweep), by row id. A run that
        # resumes a cut one reloads ``seen`` from the state; its questions must count as
        # seen too, or the close would recheck them all.
        self.seen_questions: set[str] = {r for r in self.seen if r.startswith("question:")}

    # -- requests ----------------------------------------------------------
    def _call(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        # Before the budget, so a revoked run ends as revoked, not as a resumable cut.
        if self.check_active is not None:
            self.check_active()
        self._check_budget()
        # A call the client retries on a 5xx or a network error still counts once: the
        # budget and this count are about what the run asks for, and the client owns its
        # retries.
        self.stats["requests"] += 1
        try:
            return self.client.request(method, path, check_active=self.check_active, **kwargs)
        except StackOverflowRateLimitedError:
            raise _CutShortError("rate_limit") from None
        except StackOverflowAuthError:
            if self.emitted:
                raise _CutShortError("auth") from None
            raise

    def _check_budget(self) -> None:
        if self.stats["requests"] >= self.max_requests:
            raise _CutShortError("budget")
        remaining = getattr(self.client, "quota_remaining", None)
        if remaining is not None and remaining < _QUOTA_RESERVE:
            raise _CutShortError("quota")

    # -- listings ----------------------------------------------------------
    def _questions_params(self, page: int, *, withbody: bool, fromdate: int) -> dict[str, Any]:
        params: dict[str, Any] = {
            "site": self.site,
            "page": page,
            "pagesize": PAGE_SIZE,
        }
        if self.tags:
            params["tagged"] = ";".join(self.tags)
        if fromdate:
            params["fromdate"] = fromdate
        if withbody:
            params["filter"] = WITHBODY_FILTER
        return params

    def _walk_changed(self) -> Iterator[tuple[dict[str, Any], list[dict[str, Any]]]]:
        """Yield ``(question, answers)`` for questions active since the cursor, resuming
        at the page a cut run stopped on."""
        walk = self.state.setdefault("walk", {})
        query = _hash({"tags": self.tags, "answers": "answers" in self.include})
        if walk.get("query") != query:
            walk.clear()
            walk.update({"query": query, "page": 1, "fromdate": self.state.get("cursor") or 0})
            walk["max_activity"] = self.state.get("cursor") or 0
        if walk.get("done"):
            return
        while True:
            envelope = self._call(
                "GET",
                "/questions",
                params=self._questions_params(
                    walk["page"], withbody=True, fromdate=walk["fromdate"]
                ),
            )
            questions = envelope["items"]
            answers: dict[str, list[dict[str, Any]]] = {}
            if "answers" in self.include and questions:
                answers = self._fetch_answers(questions)
            yield from ((q, answers.get(str(q["question_id"]), [])) for q in questions)
            for question in questions:
                activity = question.get("last_activity_date")
                if isinstance(activity, int) and activity > walk["max_activity"]:
                    walk["max_activity"] = activity
            if not envelope.get("has_more"):
                break
            walk["page"] += 1
        walk["done"] = True

    def _fetch_answers(self, questions: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        """The full answer lists of one page of questions, one batched endpoint per page
        of answers: a page of 100 questions can carry far more than 100 answers."""
        ids = ";".join(_valid_ids(q["question_id"] for q in questions))
        grouped: dict[str, list[dict[str, Any]]] = {}
        page = 1
        while True:
            envelope = self._call(
                "GET",
                f"/questions/{ids}/answers",
                params={
                    "site": self.site,
                    "pagesize": PAGE_SIZE,
                    "filter": WITHBODY_FILTER,
                    "page": page,
                },
            )
            for answer in envelope.get("items", []):
                grouped.setdefault(str(answer.get("question_id")), []).append(answer)
            if not envelope.get("has_more"):
                break
            page += 1
        for answers in grouped.values():
            answers.sort(key=lambda a: a["answer_id"])
        return grouped

    def _walk_sweep(self) -> Iterator[int]:
        """Yield every question id of the selection, resuming at the page a cut run
        stopped on. The default filter carries no bodies: the sweep is ids only."""
        sweep = self.state.setdefault("sweep", {})
        query = _hash({"tags": self.tags})
        if sweep.get("query") != query:
            sweep.clear()
            sweep.update({"query": query, "page": 1, "total": 0})
        if sweep.get("done"):
            return
        while True:
            envelope = self._call(
                "GET",
                "/questions",
                params=self._questions_params(sweep["page"], withbody=False, fromdate=0),
            )
            items = envelope["items"]
            sweep["total"] += len(items)
            yield from (q["question_id"] for q in items)
            if not envelope.get("has_more"):
                break
            sweep["page"] += 1
        sweep["done"] = True

    # -- rows ---------------------------------------------------------------
    def _changed_rows(self) -> Iterator[dict]:
        for question, answers in self._walk_changed():
            question_id = str(question["question_id"])
            row = {
                **render_question(question, self.site),
                NODE_SET_COLUMN: _node_set(self.site, "questions"),
            }
            fingerprint = _hash(row)
            self._mark_seen(f"question:{question_id}")
            self.stats["scanned"] += 1
            if self.fingerprints.get(row["id"]) != fingerprint:
                self.fingerprints[row["id"]] = fingerprint
                self.emitted += 1
                yield row
            else:
                self.stats["skipped"] += 1
            if "answers" in self.include:
                yield from self._answer_rows(question, answers)

    def _answer_rows(
        self, question: dict[str, Any], answers: list[dict[str, Any]]
    ) -> Iterator[dict]:
        question_id = str(question["question_id"])
        title = _text(question.get("title")) or "Untitled question"
        known = set(self.question_answers.get(question_id, []))
        current = []
        for answer in answers:
            answer_id = str(answer["answer_id"])
            current.append(answer_id)
            row = {
                **render_answer(answer, title, question_id, self.site),
                NODE_SET_COLUMN: _node_set(self.site, "answers"),
            }
            fingerprint = _hash(row)
            self.stats["scanned"] += 1
            if self.fingerprints.get(row["id"]) != fingerprint:
                self.fingerprints[row["id"]] = fingerprint
                self.emitted += 1
                yield row
            else:
                self.stats["skipped"] += 1
        # Answers that left the question's answer list are gone (or the question's body
        # was re-fetched because the question itself changed). Tombstone the missing ones.
        for answer_id in sorted(known - set(current)):
            self.fingerprints.pop(f"answer:{answer_id}", None)
            self.stats["deleted"] += 1
            yield {"id": f"answer:{answer_id}", "_deleted": True}
        self.question_answers[question_id] = sorted(current)

    def _mark_seen(self, row_id: str) -> None:
        self.seen.append(row_id)
        if row_id.startswith("question:"):
            self.seen_questions.add(row_id)

    # -- forget-on-delete ---------------------------------------------------
    def _known_question_ids(self) -> set[str]:
        known = set(self.question_answers)
        known.update(
            row_id.removeprefix("question:")
            for row_id in self.fingerprints
            if row_id.startswith("question:")
        )
        return known

    def _close_cycle(self) -> Iterator[dict]:
        """Sweep the full id set of the selection, advance the cursor, then check the
        questions the sweep missed."""
        for question_id in self._walk_sweep():
            self._mark_seen(f"question:{question_id}")
        walk = self.state.get("walk") or {}
        if walk.get("done"):
            self.state["cursor"] = walk.get("max_activity") or 0
        sweep = self.state.get("sweep") or {}
        if not sweep.get("done"):
            return
        total = sweep.get("total") or 0
        if total == 0 and self.fingerprints:
            # A selection that now matches nothing (a renamed tag, a typo in the config)
            # looks exactly like "everything was deleted". It is not trusted. The sweep is
            # discarded so the next run lists it again; the cursor stays where the
            # completed incremental listing put it.
            self.stats["failed"] += 1
            self.stats["failed_sweep"] = 1
            self.state["sweep"] = {}
            self.state["seen"] = []
            self.seen.clear()
            self.seen_questions.clear()
            logger.warning(
                "Stack Overflow sweep returned no questions while records are synced; "
                "the tag selection is not trusted and nothing was forgotten."
            )
            return
        missing_rows = sorted(
            {f"question:{qid}" for qid in self._known_question_ids()} - self.seen_questions
        )
        missing = [row_id.removeprefix("question:") for row_id in missing_rows]
        for start in range(0, len(missing), ID_BATCH):
            yield from self._check_questions(missing[start : start + ID_BATCH])
        if "answers" not in self.include:
            # A deselected kind leaves memory, without a request.
            for row_id in sorted(r for r in self.fingerprints if r.startswith("answer:")):
                self.fingerprints.pop(row_id, None)
                self.stats["deleted"] += 1
                yield {"id": row_id, "_deleted": True}
            self.question_answers.clear()
        # The cycle is closed: the next run starts a fresh listing and sweep.
        self.state["walk"] = {}
        self.state["sweep"] = {}
        self.state["seen"] = []
        self.seen.clear()
        self.seen_questions.clear()

    def _check_questions(self, question_ids: list[str]) -> Iterator[dict]:
        readable = [qid for qid in question_ids if _RECORD_ID.fullmatch(qid)]
        skipped = [qid for qid in question_ids if qid not in readable]
        for qid in skipped:
            self._check_failed(qid, StackOverflowAPIError("a question id was not numeric"))
        if not readable:
            return
        try:
            envelope = self._call(
                "GET",
                f"/questions/{';'.join(readable)}",
                params={"site": self.site, "pagesize": PAGE_SIZE},
            )
        except StackOverflowAPIError as exc:
            # A batch check that fails keeps every record in it: a 5xx or an unreadable
            # answer says nothing about any one question. They are re-checked next run.
            self._check_failed(f"{len(readable)} questions", exc)
            return
        returned = {str(item.get("question_id")): item for item in envelope.get("items", [])}
        for qid in readable:
            item = returned.get(qid)
            if item is not None and self._in_scope(item):
                continue
            yield from self._forget_question(qid)

    def _in_scope(self, question: dict[str, Any]) -> bool:
        if not self.tags:
            return True
        return bool(set(question.get("tags") or []) & set(self.tags))

    def _check_failed(self, label: str, exc: Exception) -> None:
        self.stats["failed"] += 1
        self.stats["failed_deletion_check"] = self.stats.get("failed_deletion_check", 0) + 1
        logger.warning("Stack Overflow deletion check failed for %s; it is kept: %s", label, exc)

    def _forget_question(self, question_id: str) -> Iterator[dict]:
        if f"question:{question_id}" in self.fingerprints:
            self.fingerprints.pop(f"question:{question_id}", None)
            self.stats["deleted"] += 1
            yield {"id": f"question:{question_id}", "_deleted": True}
        for answer_id in self.question_answers.pop(question_id, []):
            row_id = f"answer:{answer_id}"
            if row_id in self.fingerprints:
                self.fingerprints.pop(row_id, None)
                self.stats["deleted"] += 1
                yield {"id": row_id, "_deleted": True}

    def rows(self) -> Iterator[dict]:
        yield from self._changed_rows()
        yield from self._close_cycle()


def _iter_rows(
    client: Any,
    state: dict,
    stats: dict[str, int],
    *,
    site: str,
    tags: tuple[str, ...] = (),
    include: tuple[str, ...] = KINDS,
    max_requests: int = DEFAULT_MAX_REQUESTS,
    check_active: Callable[[], None] | None = None,
) -> Iterator[dict]:
    """Yield the changed rows of a (site, tag selection). Pure of dlt, so tests drive it
    with a dict."""
    walker = _Walker(
        client,
        state,
        stats,
        site=site,
        tags=tags,
        include=include,
        max_requests=max_requests,
        check_active=check_active,
    )
    try:
        yield from walker.rows()
    except _CutShortError as cut:
        stats["failed"] += 1
        stats[f"failed_{cut.reason}"] = 1
        logger.warning(
            "Stack Overflow sync stopped early (%s); it resumes on the next run.", cut.reason
        )
    except StackOverflowSourceError as exc:
        # A listing the key cannot call, or a key rejected before any row, ends the run
        # with a raise instead of a cut: a cut run keeps its state, so a listing already
        # marked done stays done, the refused one never finishes and the cycle never
        # closes, so no later delete is seen. The host reads these counts after the raise:
        # dlt drops the state, not this object.
        stats["failed"] += 1
        reason = next((k for t, k in _RAISED_REASONS.items() if isinstance(exc, t)), None)
        if reason:
            stats[reason] = stats.get(reason, 0) + 1
        raise


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------
def _tag_list(name: str, tags: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    """``tags`` sorted, without repeats or blanks; ``None`` is none.

    Anything but a list or tuple of strings is refused. A bare string would be read as
    one tag per letter, and a tag that matches nothing makes a run see every synced
    record as deleted (guarded by the empty-sweep check, but refused here anyway).
    """
    if tags is None:
        return ()
    if not isinstance(tags, (list, tuple)) or not all(isinstance(t, str) for t in tags):
        raise ValueError(f'{name} must be a list or tuple of tag strings, like ["python"].')
    cleaned = tuple(sorted({t.strip() for t in tags if t.strip()}))
    if tags and not cleaned:
        raise ValueError(f"{name} holds only blank strings")
    return cleaned


def stack_overflow_source(
    *,
    resource_name: str,
    site: str = "stackoverflow",
    tags: list[str] | tuple[str, ...] | None = None,
    api_key: str | None = None,
    service: Any = None,
    check_active: Callable[[], None] | None = None,
    include: tuple[str, ...] | None = None,
    max_requests: int = DEFAULT_MAX_REQUESTS,
):
    """Return a ``dlt`` resource that yields a tag selection's documents for ``remember``.

    Args:
        resource_name: Stable name of this connection, one per (site, tag selection). It
            scopes the sync state and the cleanup of deleted records, so keep it fixed
            across runs. There is no default: two selections sharing a name would forget
            each other's records.
        site: The Stack Exchange site id, e.g. ``"stackoverflow"`` or ``"serverfault"``.
        tags: Only questions carrying all of these tags (the API's ``;`` semantics).
            Without tags the source syncs every question of the site: the daily quota is
            gone in minutes. Always pass tags.
        api_key: Stack Exchange API ``key``. Optional; without it the daily quota is 300
            calls. Used to build the client when ``service`` is omitted.
        service: Pre-built client (see :func:`build_stack_overflow_service`). Mainly an
            injection point for tests and hosts. Its ``request`` takes a ``check_active``
            keyword and must call it before each request attempt, retries and waits included.
        check_active: Optional host authorization checkpoint, called before each request.
        include: Which of ``"questions"`` and ``"answers"`` to sync. ``"questions"`` is
            always required (answers are read through their question); leaving
            ``"answers"`` out forgets the synced answers. Defaults to both.
        max_requests: Budget of API calls in one run; a call the client retries counts
            once. A bigger selection finishes over several runs.

    Returns:
        A ``dlt`` resource configured with ``primary_key="id"``,
        ``write_disposition="merge"`` and a ``_deleted`` hard-delete column.
    """
    import dlt

    dlt_utils.require_document_sync_version(
        2, provider="Stack Overflow", feature="reads per-row node sets"
    )
    if not isinstance(resource_name, str) or not resource_name.strip():
        raise ValueError(
            "stack_overflow_source needs a resource_name, one per (site, tags) selection"
        )
    if not isinstance(site, str) or not site.strip():
        raise ValueError(
            "stack_overflow_source needs a Stack Exchange site id, e.g. 'stackoverflow'"
        )
    if service is None and api_key is None:
        raise ValueError("stack_overflow_source needs a service or an api_key")
    kinds = KINDS if include is None else tuple(include)
    if not kinds or "questions" not in kinds or set(kinds) - set(KINDS):
        raise ValueError(
            f'include must contain "questions" and optionally "answers", a subset of {KINDS}'
        )
    selection = _tag_list("tags", tags)

    # Built here so a malformed key fails at construction and the closure holds no raw key.
    client = service if service is not None else StackOverflowClient(api_key)
    stats: dict[str, int] = {}

    @dlt.resource(
        name=resource_name,
        primary_key="id",
        write_disposition="merge",
        # cognee_node_set needs no json hint here: ingest_dlt_source applies
        # DOCUMENT_COLUMN_HINTS to every document source, merged with this one.
        columns={"_deleted": {"data_type": "bool", "hard_delete": True}},
    )
    def stack_overflow_selection():
        stats.clear()
        rows = _iter_rows(
            client,
            dlt.current.resource_state(),
            stats,
            site=site.strip(),
            tags=selection,
            include=kinds,
            max_requests=max_requests,
            check_active=check_active,
        )
        yield from dlt_utils.guarded_rows(rows, check_active)

    resource = stack_overflow_selection()
    setattr(resource, DOCUMENT_SOURCE_ATTR, "stack_overflow")
    setattr(resource, dlt_utils.PIPELINE_SCOPE_ATTR, resource_name)
    resource.cognee_sync_stats = stats
    return resource

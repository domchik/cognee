"""In-memory stand-in for the Stack Exchange API, shaped like its real responses.

An endpoint the connector does not use is not answered: a call to it raises
``AssertionError`` and fails the test that made it.
"""

PAGE_SIZE = 100


class FakeStackExchange:
    """Answers ``/questions``, ``/questions/{ids}`` and ``/questions/{ids}/answers`` the
    way the 2.3 API does: envelope with ``items``/``has_more``, ``fromdate`` filtering on
    ``last_activity_date``, ``tagged`` with AND semantics, and an optional ``backoff``."""

    def __init__(self, site: str = "stackoverflow"):
        self.site = site
        self.questions: dict[int, dict] = {}
        self.answers: dict[int, dict] = {}
        self.calls: list[tuple[str, dict]] = []
        self.bodies: list[tuple[str, dict]] = []
        # path -> exception raised for it, like a 500 on one batch read
        self.fail_paths: dict[str, Exception] = {}
        self.fail_after: int | None = None
        self.fail_with: Exception | None = None
        self.quota_remaining = 9999
        self.backoff: int | None = None

    # -- seeding -----------------------------------------------------------
    def _next_activity(self) -> int:
        activities = [q["last_activity_date"] for q in self.questions.values()]
        activities += [a["last_activity_date"] for a in self.answers.values()]
        return (max(activities) if activities else 1_700_000_000) + 60

    def add_question(
        self,
        question_id: int,
        title: str,
        *,
        tags: list[str],
        owner: str = "Ada Lovelace",
        score: int = 1,
        body: str = "Question body.",
        last_activity: int | None = None,
        link: str | None = None,
    ) -> dict:
        self.questions[question_id] = {
            "question_id": question_id,
            "title": title,
            "tags": tags,
            "owner": {"display_name": owner},
            "score": score,
            "answer_count": 0,
            "body_markdown": body,
            "last_activity_date": last_activity or self._next_activity(),
            "link": link or f"https://{self.site}.com/questions/{question_id}",
        }
        return self.questions[question_id]

    def add_answer(
        self,
        answer_id: int,
        question_id: int,
        *,
        owner: str = "Ben Turing",
        score: int = 1,
        accepted: bool = False,
        body: str = "Answer body.",
        last_activity: int | None = None,
    ) -> dict:
        assert question_id in self.questions
        self.answers[answer_id] = {
            "answer_id": answer_id,
            "question_id": question_id,
            "owner": {"display_name": owner},
            "score": score,
            "is_accepted": accepted,
            "body_markdown": body,
            "last_activity_date": last_activity or self._next_activity(),
        }
        self.questions[question_id]["answer_count"] += 1
        return self.answers[answer_id]

    # -- the api -----------------------------------------------------------
    def count(self, path: str) -> int:
        return sum(1 for called, _ in self.calls if called == path)

    def request(self, method: str, path: str, *, params=None, check_active=None):
        assert method == "GET"
        # check_active is enforced by the walker (and the real client before every
        # attempt); the fake records the call only.
        params = params or {}
        self.calls.append((path, dict(params)))
        if path in self.fail_paths:
            raise self.fail_paths[path]
        if self.fail_after is not None and len(self.calls) > self.fail_after:
            raise self.fail_with
        if path == "/questions":
            items = self._search(params)
            items = self._strip(items, withbody=params.get("filter") == "withbody")
            return self._page(path, items, params)
        head, _, tail = path.removeprefix("/questions/").partition("/")
        if tail == "answers":
            ids = [int(i) for i in head.split(";")]
            items = [a for qid in ids for a in self._answers_of(qid)]
            items = self._strip(items, withbody=params.get("filter") == "withbody")
            return self._page(path, items, params)
        if tail == "" and head:
            ids = [int(i) for i in head.split(";") if i.isdigit()]
            # the default filter carries no bodies but keeps the tags
            items = [
                {k: v for k, v in self.questions[qid].items() if k != "body_markdown"}
                for qid in ids
                if qid in self.questions
            ]
            return self._page(path, items, params)
        raise AssertionError(f"unexpected call {method} {path}")

    def _answers_of(self, question_id: int) -> list[dict]:
        return sorted(
            (a for a in self.answers.values() if a["question_id"] == question_id),
            key=lambda a: a["answer_id"],
        )

    def _search(self, params: dict) -> list[dict]:
        tagged = params.get("tagged")
        required = tagged.split(";") if tagged else []
        fromdate = params.get("fromdate") or 0
        return [
            q
            for q in self.questions.values()
            if all(tag in q["tags"] for tag in required) and q["last_activity_date"] >= fromdate
        ]

    @staticmethod
    def _strip(items: list[dict], *, withbody: bool) -> list[dict]:
        if withbody:
            return [dict(item) for item in items]
        return [{k: v for k, v in item.items() if k != "body_markdown"} for item in items]

    def _page(self, path: str, items: list[dict], params: dict) -> dict:
        page, pagesize = params.get("page", 1), params.get("pagesize", PAGE_SIZE)
        start = (page - 1) * pagesize
        chunk = items[start : start + pagesize]
        envelope = {
            "items": chunk,
            "has_more": start + pagesize < len(items),
            "quota_remaining": self.quota_remaining,
        }
        if self.backoff:
            envelope["backoff"] = self.backoff
        return envelope


class _HTTPResponse:
    def __init__(self, status: int, payload):
        self.status_code = status
        self._payload = payload
        self.headers = {}

    def json(self):
        return self._payload


def serve_over_http(fake: FakeStackExchange, *, keys: list[str] | None = None):
    """A stand-in for ``requests.request`` that answers from ``fake`` the way the Stack
    Exchange HTTP API does. ``keys`` collects the ``key`` query parameter of every
    request."""

    def request(method, url, *, params=None, timeout=None, **_):
        params = dict(params or {})
        if keys is not None:
            keys.append(params.get("key"))
        path = url.removeprefix("https://api.stackexchange.com/2.3")
        payload = fake.request(method, path, params=params)
        return _HTTPResponse(200, payload)

    return request

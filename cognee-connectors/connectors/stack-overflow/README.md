# cognee-connector-stack-overflow

A Stack Overflow data-source connector for [cognee](https://github.com/topoteretes/cognee):
sync the questions and answers of a tag selection into memory, incrementally, and forget
records when they are deleted upstream.

It exposes a `dlt` source you hand to `cognee.remember(...)`. Each record becomes a
**normal document** in cognee (it flows through chunking and LLM entity extraction), via
cognee's document-mode marker. It reads the public Stack Exchange API — any site of the
network, Stack Overflow by default — and never authenticates as a user, so private data
(accepted-answer notifications, voting history) is out of reach by construction. The
package also carries the provider's connection contract (`CONNECTOR`, an API-key
descriptor on `cognee-connectors-core`) for a cognee server or Cognee Cloud.

## Install

```bash
pip install cognee-connector-stack-overflow             # the connector and its DLT source
pip install "cognee-connector-stack-overflow[cognee]"   # plus cognee itself, which runs the sync
```

## Requirements

cognee 1.6.5 or later whose ingestion reads per-row node sets (`DOCUMENT_SYNC_VERSION` 2 or
later; the source refuses an older one before it makes a request). The REST client uses
`requests`, which the package declares and `dlt` (a dependency of
`cognee-connectors-core[dlt]`) requires anyway.

The Stack Exchange API is free and anonymous, but the daily quota is 300 calls without a
`key` and 10,000 with one. Register an app at
[stackapps.com](https://stackapps.com/apps/oauth/register) (no OAuth flow is needed — the
`key` alone is enough) and pass it as `api_key`.

## Configuration

This package reads no environment variable. cognee has no `STACK_OVERFLOW_*` settings and
no settings class for this provider, and the key is never read from the environment: it is
a parameter of the source factory (`api_key=`, or a pre-built `service=`). Everything is
configured on the `stack_overflow_source(...)` call, see the argument table under Usage.

## Usage

```python
import cognee
from cognee_connector_stack_overflow import stack_overflow_source

await cognee.remember(
    stack_overflow_source(
        tags=["cognee"],
        resource_name="stack_overflow_cognee",
        api_key="<stackapps key>",  # required; raises the daily quota 300 -> 10,000
    ),
    dataset_name="stack_overflow",
    primary_key="id",
    write_disposition="merge",  # required: incremental upserts and deletes
    max_rows_per_table=0,  # no row cap; orphan cleanup must see the whole corpus
)

answer = await cognee.recall("What do people ask about cognee?", datasets=["stack_overflow"])
```

A runnable version of this snippet lives in [examples/](examples/stack_overflow.py). Run
the same `remember` call again whenever you want to sync: only questions active since the
last run are listed, and only records whose text changed are re-extracted.

| Argument | Meaning | Default |
|---|---|---|
| `resource_name` | Names this connection: it keys the sync state and the cleanup of deleted records | required, one per (site, tags) selection |
| `site` | The Stack Exchange site id: `stackoverflow`, `serverfault`, `askubuntu`, ... | `stackoverflow` |
| `tags` | Only questions carrying all of these tags (the API's `;` AND semantics). **Without tags the quota is gone in minutes** | none |
| `api_key` | Stack Exchange API `key` from stackapps.com | required (the source refuses to run without one) |
| `include` | `"questions"`, or `("questions", "answers")` | both |
| `max_requests` | Budget of API calls in one run; a call the client retries counts once | 200 |
| `check_active` | Host checkpoint, called before every request | none |
| `service` | Pre-built client, from `build_stack_overflow_service(api_key)` | built from `api_key` |

`tags` takes a list or tuple of strings; anything else, a bare string included, is refused.
The tags are sorted and repeats dropped, so the same selection in another order resumes an
interrupted listing.

After a run, `source.cognee_sync_stats` holds counts only: `scanned`, `skipped`, `deleted`,
`failed` and `requests`. `requests` counts the API calls the run made.

- `failed_budget`, `failed_quota`: the run stopped early and resumes next time
- `failed_rate_limit`: the API answered with a throttle violation or HTTP 429; the run
  stops cleanly, keeps its state and resumes next time
- `failed_auth`: the API rejected the credentials. After the run emitted rows it stops
  early; before any row it raises, dlt drops the state of that run and the next run starts
  over
- `failed_sweep`: the delete-detection sweep returned zero questions while records are
  synced — the tag selection matches nothing, and nothing was forgotten
- `failed_deletion_check`: the batch check of deleted questions failed (a 5xx, an
  unreadable answer); the records are kept

## Running on a cognee server

```python
from cognee_connector_stack_overflow.cognee_host import register

register()  # a no-op: there is nothing to register for Stack Overflow
```

Stack Overflow has no OAuth flow, so cognee ships no adapter, no credential routes and no
settings for it: `register()` does nothing, `routers()` is empty, and the package declares
no `cognee.integrations` entry point. A server syncs Stack Overflow the way the SDK does,
by handing `stack_overflow_source(...)` to `remember`; a host that stores keys builds the
source with `service=build_stack_overflow_service(key)` and names the connection with
`resource_name`, as cognee's `modules.integrations.ingestion` does. `CONNECTOR` (an
API-key `ConnectorDescriptor` served by `DescriptorConnector`) names the provider for a
host's catalog, derives a connection's dataset from its account id
(`stack_overflow_<account id>` — use the site id) and hands a stored key out as the sync
credential, read from `tokens["access_token"]`; it does not enumerate a site, the source
does.

## What is ingested

| Row | Id | Title | Content |
|---|---|---|---|
| Question | `question:<id>` | question title | a sentence with the title, asked-by/tags/score/answer-count/link lines, the markdown body |
| Answer | `answer:<id>` | the question's title | a sentence naming the answerer and the question, score/accepted/link lines, the markdown body |

```
Stack Overflow question "How do I add memory to an agent?".
Asked by: Ada
Tags: cognee, python
Score: 12
Answers: 2
Link: https://stackoverflow.com/questions/12345

<body markdown>
```

An answer names its question by the question's own title, so the graph joins them:
`Answer by Ben to "How do I add memory to an agent?".` Ids are never rendered; people are
rendered by display name only, and two people with the same name are one entity in the
graph — that is how cognee merges entities and a known limit of rendering each record as
text. Comments are not read, and the body of a question or answer is public data that the
site shows to everyone.

## Node set and scoping a recall

Each row carries `cognee_node_set`: `["stack_overflow:<site>:<kind>"]`, with `<kind>` one of
`questions`, `answers`. The name already starts with the `stack_overflow:` source tag, so
cognee keeps it as it is, and the record's document, chunks and entities belong to that
node set. Scope a recall to Stack Overflow, to one site, or to one kind:

```python
await cognee.recall(
    "What do people ask about cognee?",
    datasets=["stack_overflow"],
    node_name=["stack_overflow:stackoverflow:questions"],
)
```

The node set is part of a document's identity, like every other column this source emits.
It is fixed at the first release: changing it, or the wording of the text above, gives
every record a new document and one more extraction on your LLM key. A tombstone carries
no node set.

## How sync and forget-on-delete work

- **Incremental cursor:** the Stack Exchange API filters by `fromdate` on a record's last
  activity. The first run lists the whole selection; each later run lists only questions
  active since the stored cursor, and a row is emitted only when its rendered text changed
  (a fingerprint kept in dlt's per-resource state). The cursor moves when a run completes
  its listing, to the highest `last_activity_date` seen.
- **Answers are read through their question:** for each page of changed questions the
  source fetches the questions' full answer lists in one batched request. A new answer
  bumps the question's activity, so the question is re-listed and the answer shows up. An
  answer that vanished from the list (deleted, or moved with its question) is forgotten
  then. An answer edit bumps the question's activity on Stack Overflow, so edits are
  picked up with the question.
- **Deleted questions:** the listings never contain deleted records, so a run that
  completed its incremental listing sweeps the full id set of the tag selection (bodies
  excluded) and batch-reads the missing ids, 100 per request. A question that no longer
  exists, or that lost the selected tags, is forgotten together with its answers. A check
  that fails (a 5xx, an unreadable answer) keeps the record and counts
  `failed_deletion_check`.
- **Tombstones:** forgotten records are emitted as `{"id": ..., "_deleted": True}` with a
  `hard_delete` column. dlt removes them on `merge`, and cognee's orphan cleanup removes
  them from the graph and vector stores.
- **An empty sweep is not trusted:** a tag selection that now matches nothing looks exactly
  like "everything was deleted". When a completed sweep returns zero questions while
  records are synced, the run reports `failed_sweep` and forgets nothing.
- **Rate limits and quota:** every answer carries `quota_remaining` and may carry
  `backoff` (seconds to wait before the next call); the client honors `backoff` and a run
  stops cleanly when fewer than 10 calls are left in the daily quota, on a throttle
  violation, or at `max_requests` — keeping its progress, reporting the reason, and
  resuming on the next run. It never waits out a throttle: `dlt` rolls the state of a
  failed run back, so a selection too big for one window would never finish.
- **A redirect is an error.** An answer with a 3xx status raises `StackOverflowAPIError`
  naming the status and the endpoint, and is not followed: the request carries the key in
  the query string, which `requests` would send to whatever host the answer names.
- `check_active` (for hosts) runs before every API request.

A completed run over a settled selection costs one page of questions (usually empty after
the first sync) plus the id sweep: one request per 100 questions in the selection per run,
plus one per 100 deleted questions. Budget accordingly against the daily quota.

## Rate limits

The quota is per-day: 300 calls without a `key`, 10,000 with one, shared by every app
using the key. Heavy endpoints (search) may additionally answer with `backoff`, which the
client honors by sleeping before its next call. The run budget (`max_requests`, default
200) and the 10-call reserve against `quota_remaining` keep one sync from spending the
whole day on a too-broad selection; the note in the issue that became this connector is
worth repeating: filter by tag, or the quota is gone in minutes.

## Privacy

This connector reads public data only: it never authenticates as a user, so nothing
private to an account is reachable.

- **What is read:** questions and answers of the selected tags, including their markdown
  bodies, scores and authors' display names — all fields the site shows publicly.
- **Where it goes:** the text becomes cognee documents. During cognify it is sent to your
  configured LLM and embedding providers, like any document you add to cognee.
- **dlt state** stores the cursor, each record's id with a fingerprint (a hash of its
  rendered text), and per-question answer ids. It stores no text. Document ids contain the
  Stack Exchange record id.
- **dlt's staging database** for the dataset (`dlt_database_<dataset>`) holds the whole
  rendered text of every record next to that state, and dlt also keeps every completed
  load package with the rendered text of each row it loaded in its data directory
  (`~/.dlt` of the server process unless `DLT_DATA_DIR` moves it). Anyone with read access
  to the dataset can see the text; keep it in its own dataset and share it accordingly.
- **Logs and `cognee_sync_stats`** contain counts, statuses and endpoints. The key is
  never logged or put in an error: it is a validated query parameter, and a redirect is
  never followed.
- **How deletion is honoured:** a question or answer deleted on the site is forgotten on
  the **next sync**, as part of `remember`. A record whose check keeps failing stays in
  memory until a check succeeds. A deleted question's text stays in the completed dlt load
  packages described above: erasing it for good means deleting those packages and the
  staging database yourself, a known limit of cognee's dlt ingestion, not of this
  connector.

## Development

From a `cognee-connectors` checkout, with cognee installed (the workspace's `dev` group
has it):

```bash
export COGNEE_ENV_FILE=<an empty file> TELEMETRY_DISABLED=1 LITELLM_MODE=PRODUCTION LLM_API_KEY=sk-test-dummy
PYTHONPATH=core:connectors/stack-overflow python -m pytest connectors/stack-overflow/tests -q
python scripts/check_cognee_free.py cognee_connector_stack_overflow
```

No Stack Exchange account or key is needed. `tests/unit/fake_stack_overflow.py` is an
in-memory Stack Exchange API shaped like its responses: paged listings with `has_more`,
`fromdate` filtering, batched id reads and quota/backoff envelopes. It answers no endpoint
the connector does not use, so a call to one fails the test.

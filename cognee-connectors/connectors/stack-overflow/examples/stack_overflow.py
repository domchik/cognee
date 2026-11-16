"""Stack Overflow connector demo — "ask a tag".

Pull the questions and answers of a Stack Overflow tag into cognee memory,
incrementally, with forget-on-delete.

This example is built on cognee's DLT ingestion subsystem:
``stack_overflow_source`` returns a ``dlt`` resource that you hand straight to
``cognee.remember``. The first run loads every question of the tag; re-running
``remember`` lists only questions active since the last run, and questions and
answers deleted upstream are forgotten from memory on the next sync.

────────────────────────────────────────────────────────────────────────────
One-time setup
────────────────────────────────────────────────────────────────────────────
1. Install the connector:

       pip install "cognee-connector-stack-overflow[cognee]"

2. Register an app at https://stackapps.com/apps/oauth/register and export
   its key — the source requires one (the anonymous quota of 300 calls/day
   is too small for even a single tag):

       export STACK_OVERFLOW_API_KEY=<key>

3. Pick a tag (small ones keep the quota honest):

       export STACK_OVERFLOW_TAG=cognee

4. Set your LLM key (``LLM_API_KEY``) in ``.env`` like any other cognee example.

Run it:

    python connectors/stack-overflow/examples/stack_overflow.py
"""

import asyncio
import os

import cognee

from cognee_connector_stack_overflow import stack_overflow_source

# Keep the tag in its own dataset so it is easy to inspect and forget.
DATASET_NAME = "stack_overflow_demo"

# Routing kwargs for remember(). Pass the same ones on every later sync.
#   write_disposition="merge" is REQUIRED: the add pipeline defaults to
#     "replace", which would wipe the whole synced selection on the next sync.
#   max_rows_per_table=0 guarantees no per-table read cap applies (even if
#     DLT_MAX_ROWS_PER_TABLE is set), so orphan-cleanup (forget-on-delete)
#     compares against the *entire* synced corpus.
STACK_OVERFLOW_REMEMBER_KWARGS = {
    "primary_key": "id",
    "write_disposition": "merge",
    "max_rows_per_table": 0,
    "self_improvement": False,
}


async def main():
    tags = [os.environ.get("STACK_OVERFLOW_TAG", "cognee")]
    api_key = os.environ.get("STACK_OVERFLOW_API_KEY") or None

    # Start from a clean slate so the demo is reproducible.
    await cognee.forget(dataset_name=DATASET_NAME)

    await cognee.remember(
        stack_overflow_source(
            tags=tags,
            resource_name="stack_overflow_" + tags[0],
            api_key=api_key,
        ),
        dataset_name=DATASET_NAME,
        **STACK_OVERFLOW_REMEMBER_KWARGS,
    )

    answer = await cognee.recall(
        f"What do people ask about {tags[0]}?",
        datasets=[DATASET_NAME],
    )
    print(answer)


if __name__ == "__main__":
    asyncio.run(main())

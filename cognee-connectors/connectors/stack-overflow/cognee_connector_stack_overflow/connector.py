"""The Stack Overflow connector on the shared contract: ``CONNECTOR``.

Stack Overflow authenticates with a Stack Exchange API ``key`` the caller hands to
:func:`cognee_connector_stack_overflow.source.stack_overflow_source`: there is no OAuth
flow, no webhook and no host adapter in cognee. So the descriptor is an API-key one
(``auth="api_key"``): the OAuth members of ``DescriptorConnector`` raise
``NotImplementedError``, ``refresh`` returns ``None``, and what works is what a host and
the catalog need: the provider's name, a connection's dataset
(``stack_overflow_<account id>``, the site id a host stores as the account id, see the
README) and ``sync_scope``, which hands a stored key out as the sync credential from
``tokens["access_token"]`` for the resources a host names. It does not enumerate a site:
the source lists it, one ``resource_name`` per (site, tag selection), so a host builds
the source itself (``stack_overflow_source(service=build_stack_overflow_service(key),
...)``), the way cognee's ``modules.integrations.ingestion`` does. Nothing in this module
imports cognee.
"""

from cognee_connectors_core.descriptor import ConnectorDescriptor
from cognee_connectors_core.oauth2 import DescriptorConnector

DESCRIPTOR = ConnectorDescriptor(
    provider="stack_overflow",
    display_name="Stack Overflow",
    auth="api_key",
    # No webhooks: Stack Exchange has no change feed for questions, the source lists
    # the selection incrementally by activity and sweeps it for deletions. No poll
    # interval either: a sync runs when its caller runs it, as in cognee; a host that
    # schedules syncs picks the cadence against the per-day quota.
)

# No settings object: the key is a parameter of the source factory and is never read from
# the environment (settings.py). The default HTTP client is never used: the OAuth members,
# the only ones that would call the provider, refuse before any request.
CONNECTOR = DescriptorConnector(DESCRIPTOR)

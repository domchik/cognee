"""Contract tests for the Stack Overflow connector.

Stack Overflow is an API-key provider, so there is no token response to replay through
the contract. These pin what the contract object does instead: it names the provider for
hosts and the catalog, derives a connection's dataset from its account id, hands a stored
key out as the sync credential, refuses the OAuth members, and never calls Stack
Exchange (the scripted ``FakeHttpClient`` records no request).
"""

import pytest

from cognee_connector_stack_overflow import (
    CONNECTOR,
    build_stack_overflow_service,
    stack_overflow_source,
)
from cognee_connector_stack_overflow.connector import DESCRIPTOR
from cognee_connectors_core import registry
from cognee_connectors_core.oauth2 import DescriptorConnector
from cognee_connectors_core.testing import FakeCredential, FakeHttpClient

KEY = "SECRET-stackapps-key-123"
# What a host stores for a connection: the Stack Exchange site id as the account id, and
# the key under the field DescriptorConnector reads a bearer credential from.
CREDENTIAL = FakeCredential(
    provider="stack_overflow",
    provider_account_id="stackoverflow",
    account_label=None,
    auth_type="api_key",
)
TOKENS = {"access_token": KEY}


@pytest.fixture(autouse=True)
def _empty_registry():
    registry.reset_registry()
    yield
    registry.reset_registry()


def _connector(http: FakeHttpClient) -> DescriptorConnector:
    return DescriptorConnector(DESCRIPTOR, http=http)


def test_descriptor_identity():
    assert DESCRIPTOR.provider == "stack_overflow"
    assert DESCRIPTOR.display_name == "Stack Overflow"
    assert DESCRIPTOR.auth == "api_key"
    assert DESCRIPTOR.signature_scheme is None
    assert DESCRIPTOR.poll_interval_seconds is None
    assert DESCRIPTOR.content_type is None


def test_connector_serves_the_descriptor():
    assert isinstance(CONNECTOR, DescriptorConnector)
    assert CONNECTOR.descriptor is DESCRIPTOR
    assert CONNECTOR.provider == DESCRIPTOR.provider
    assert CONNECTOR.display_name == DESCRIPTOR.display_name
    assert CONNECTOR.auth == DESCRIPTOR.auth
    assert CONNECTOR.settings is None


@pytest.mark.asyncio
async def test_the_oauth_members_refuse_and_stack_overflow_is_never_called():
    http = FakeHttpClient()
    connector = _connector(http)

    with pytest.raises(NotImplementedError, match="api_key"):
        connector.authorize_url("state-1")
    with pytest.raises(NotImplementedError, match="api_key"):
        await connector.exchange_callback("code", {})
    with pytest.raises(NotImplementedError, match="api_key"):
        connector.parse_installation({"access_token": KEY})
    assert await connector.refresh(CREDENTIAL, TOKENS) is None
    await connector.revoke_remote(CREDENTIAL, TOKENS)
    assert http.calls == []


@pytest.mark.asyncio
async def test_sync_scope_names_the_sites_dataset_and_hands_the_stored_key_out():
    http = FakeHttpClient()

    scope = await _connector(http).sync_scope(CREDENTIAL, TOKENS, resources=["questions"])

    assert scope.dataset_name == "stack_overflow_stackoverflow"
    assert scope.items == ["questions"]
    assert scope.credential == KEY
    assert scope.content_type is None
    assert http.calls == []


@pytest.mark.asyncio
async def test_a_full_sync_is_the_sources_job_not_the_descriptors():
    """A descriptor cannot enumerate a site; stack_overflow_source lists it, one
    resource_name per (site, tag selection)."""
    with pytest.raises(NotImplementedError):
        await _connector(FakeHttpClient()).sync_scope(CREDENTIAL, TOKENS)


def test_the_dataset_is_derived_from_the_account_id():
    assert CONNECTOR.dataset_name(CREDENTIAL) == "stack_overflow_stackoverflow"
    other = FakeCredential(provider="stack_overflow", provider_account_id="Server Fault")
    assert CONNECTOR.dataset_name(other) == "stack_overflow_server_fault"


def test_no_webhooks_no_polling_no_picker():
    assert CONNECTOR.webhook_verifier() is None
    assert CONNECTOR.webhook_delivery_key(b"{}", {}) is None
    assert CONNECTOR.poll_interval_seconds is None
    assert CONNECTOR.poll_targets(CREDENTIAL) is None
    assert type(CONNECTOR).supports_resource_selection() is False


def test_the_catalog_lists_stack_overflow_as_an_api_key_provider():
    registry.use_connector(CONNECTOR)

    assert registry.get_connector("stack_overflow") is CONNECTOR
    assert registry.catalog() == [
        {
            "provider": "stack_overflow",
            "display_name": "Stack Overflow",
            "auth": "api_key",
            "tier": 1,
            "has_webhooks": False,
            "supports_resource_selection": False,
            "poll_interval_seconds": None,
            "content_type": None,
            "degraded": False,
        }
    ]


def test_the_bare_descriptor_registers_too():
    """What an entry point may hand a host: use_connector wraps it the same way."""
    registered = registry.use_connector(DESCRIPTOR)

    assert isinstance(registered, DescriptorConnector)
    assert registered.descriptor is DESCRIPTOR


def test_the_package_exports_what_the_entry_points_name():
    assert stack_overflow_source.__module__ == "cognee_connector_stack_overflow.source"
    assert build_stack_overflow_service.__module__ == "cognee_connector_stack_overflow.source"
    assert callable(stack_overflow_source) and callable(build_stack_overflow_service)


def test_the_source_factory_validates_its_arguments():
    with pytest.raises(ValueError, match="resource_name"):
        stack_overflow_source(resource_name=" ", api_key=KEY)
    with pytest.raises(ValueError, match="site"):
        stack_overflow_source(resource_name="x", site=" ", api_key=KEY)
    with pytest.raises(ValueError, match="service or an api_key"):
        stack_overflow_source(resource_name="x")
    with pytest.raises(ValueError, match="questions"):
        stack_overflow_source(resource_name="x", api_key=KEY, include=("answers",))
    with pytest.raises(ValueError, match="list or tuple"):
        stack_overflow_source(resource_name="x", api_key=KEY, tags="python")
    with pytest.raises(ValueError, match="blank"):
        stack_overflow_source(resource_name="x", api_key=KEY, tags=[" "])


def test_the_source_factory_wraps_the_walk_in_a_dlt_resource():
    pytest.importorskip("dlt")
    from cognee_connector_stack_overflow.source import DOCUMENT_SOURCE_ATTR

    resource = stack_overflow_source(resource_name="so_cognee", tags=["cognee"], api_key=KEY)

    assert resource.name == "so_cognee"
    assert getattr(resource, DOCUMENT_SOURCE_ATTR) == "stack_overflow"
    assert resource.cognee_sync_stats == {}

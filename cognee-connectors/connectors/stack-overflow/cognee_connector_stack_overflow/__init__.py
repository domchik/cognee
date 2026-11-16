"""Stack Overflow connector for cognee.

Exports the contract object (``CONNECTOR``), the DLT source factory
(``stack_overflow_source``) and the service builder a host wraps an API key in
(``build_stack_overflow_service``), the two names ``cognee.tasks.ingestion.connectors``
exported. This module imports no host and no client library: cognee is needed only by
:mod:`cognee_connector_stack_overflow.cognee_host`, which is installed with the
``cognee`` extra, and ``requests`` is loaded on the first request.
"""

from cognee_connector_stack_overflow.connector import CONNECTOR
from cognee_connector_stack_overflow.source import (
    build_stack_overflow_service,
    stack_overflow_source,
)

__all__ = ["CONNECTOR", "build_stack_overflow_service", "stack_overflow_source"]

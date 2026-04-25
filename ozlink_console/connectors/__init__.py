"""
Connector abstraction for multi-provider relocation (MVP: contracts + stubs).

Execution stays in the customer trust boundary (desktop agent); a future SaaS control plane
orchestrates jobs while connectors perform provider-specific I/O without Ozlink hosting file bytes.
"""

from ozlink_console.connectors.base import ConnectorCapabilities, RelocatorConnector

__all__ = ["ConnectorCapabilities", "RelocatorConnector"]

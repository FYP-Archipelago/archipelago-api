"""The service's own version.

Distinct from ``contracts.API_SCHEMA_VERSION``, and the two move independently:
the schema version is a promise to clients about the shape of a response, while
this is a release marker for the service, tracking the frontend's v0.1/v0.2/…
convention. A release that only changes internals bumps this and not that.
"""

__version__ = "v0.1"

"""Shared exception types for the Nubra ingestion auth layer.

Pulled out into its own module so :mod:`app.ingestion.input_patch`
(installed at module-top in :mod:`app.main`) and
:mod:`app.ingestion.auth_client` (uses the patch) can both import it
without a circular dependency.
"""

from __future__ import annotations


class NubraAuthError(RuntimeError):
    """Raised when session-only Nubra authentication cannot proceed.

    Examples
    --------
    * ``auth_data.db.*`` is missing entirely (no enrolment was ever
      performed, or the volume is not mounted).
    * The shelve exists but is missing one of ``auth_token`` /
      ``session_token`` / ``x-device-id``.
    * The cached ``session_token`` JWT has expired and ops has not
      refreshed ``auth_data.db.*`` externally.
    * The SDK fell into an unexpected stdin-reading code path. The
      input guard installed at startup raises this rather than block.
    """

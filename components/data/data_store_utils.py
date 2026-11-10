#!/usr/bin/env python3
"""
Data Store Utilities Module

This module provides centralized data store management utilities for the Ahead of the Storm application.
It consolidates data store initialization logic and provides a single source of truth for data store configuration.

Key Components:
- Centralized data store initialization based on environment variables
- Consistent data store configuration across the application
- Support for LocalDataStore, ADLSDataStore, and SnowflakeDataStore

Usage:
    from components.data.data_store_utils import get_data_store
    data_store = get_data_store()
"""

import logging
import threading
import time

logger = logging.getLogger(__name__)

# Import centralized configuration
from components.config import config as app_config


class _LazyDataStore:
    """Thin proxy that defers constructing the real gigaspatial DataStore
    (and therefore its `gigaspatial` import, which pulls in geemap/hdx/
    sklearn) until the first attribute access.

    Several callers (e.g. page modules) call `get_data_store()`
    unconditionally at module import time. Simply moving the `gigaspatial`
    import inside `get_data_store()` does not, by itself, defer anything for
    those callers: the import would still run the moment the module-level
    call executes. Wrapping the real store behind this proxy means the import
    only happens if a `.file_exists()`/`.read_file()`/`.open()`/etc call is
    actually made on it.
    """

    def __init__(self, factory):
        self._factory = factory
        self._store = None
        self._lock = threading.Lock()

    def _ensure(self):
        store = self._store
        if store is None:
            with self._lock:
                store = self._store
                if store is None:
                    store = self._factory()
                    self._store = store
        return store

    def __getattr__(self, name):
        # Only reached for attributes not already found on this proxy
        # instance itself (factory/_store/_lock), so this can't recurse.
        return getattr(self._ensure(), name)


def get_data_store():
    """
    Get the appropriate data store based on centralized configuration.

    This controls where pre-processed impact views are stored:
    - LOCAL: Local filesystem (default)
    - BLOB: Azure Blob Storage (read-only, this app only reads data)
    - SNOWFLAKE: Snowflake internal stage (read-only, this app only reads data)

    Note: Snowflake can be used for BOTH raw hurricane forecast data (tables) AND impact views (stages).

    Returns:
        DataStore: Configured data store instance (a lazy proxy, see
        `_LazyDataStore`, that only imports/constructs the real gigaspatial
        store on first actual use)
    """
    # GigaSpatial imports pull in geemap/hdx/sklearn and are only needed for
    # file-store reads. Deferred via _LazyDataStore so any caller that never
    # touches the store never pays this cost, even though this factory itself
    # typically runs at module-import time.
    impact_data_store = app_config.IMPACT_DATA_STORE

    def _build():
        if impact_data_store == 'BLOB':
            from gigaspatial.core.io.adls_data_store import ADLSDataStore
            return ADLSDataStore(
                container=app_config.ADLS_CONTAINER_NAME,
                account_url=app_config.ADLS_ACCOUNT_URL,
                sas_token=app_config.ADLS_SAS_TOKEN,
            )
        elif impact_data_store == 'SNOWFLAKE':
            try:
                from gigaspatial.core.io.snowflake_data_store import SnowflakeDataStore
            except ImportError:
                SnowflakeDataStore = None
            if SnowflakeDataStore is None:
                raise ImportError(
                    "SnowflakeDataStore not available. Please ensure giga-spatial>=0.7.0 is installed "
                    "and includes the SnowflakeDataStore class."
                )

            # Note: SnowflakeDataStore uses standard password authentication
            # SPCS OAuth is not currently supported for Snowflake stages
            # Use password authentication (SPCS_RUN=false) for Snowflake stage access
            return SnowflakeDataStore(
                account=app_config.SNOWFLAKE_ACCOUNT,
                user=app_config.SNOWFLAKE_USER,
                password=app_config.SNOWFLAKE_PASSWORD,
                warehouse=app_config.SNOWFLAKE_WAREHOUSE,
                database=app_config.SNOWFLAKE_DATABASE,
                schema=app_config.SNOWFLAKE_SCHEMA,
                stage_name=app_config.SNOWFLAKE_STAGE_NAME
            )
        else:
            # Default to local storage
            from gigaspatial.core.io.local_data_store import LocalDataStore
            return LocalDataStore()

    return _LazyDataStore(_build)


# --- Blob-first read, with a Snowflake-stage fallback, for the two global raw
# --- hazard layers ---
#
# New met/ writes (precip tp, river ro) land in Azure Blob storage (mirrored
# into Snowflake only as the AOTS_ANALYSIS_BLOB EXTERNAL stage), while this
# app's configured SNOWFLAKE_STAGE_NAME points at the INTERNAL AOTS_ANALYSIS
# stage. AOTS_ANALYSIS_BLOB is not a mirror of AOTS_ANALYSIS: it is a strict
# subset that holds only what has been written since each pipeline's cutover.
# Every recent precip-raw/river-raw forecast cycle therefore exists only in
# Blob, while older data (geodb/aos_views, eval, project_results, glofas,
# etc.) lives only in the internal stage. This tries Blob first (the common
# case for current cycles) and falls back to the internal stage only for
# older data. Switching SNOWFLAKE_STAGE_NAME globally would break every other
# read of anything only in the internal stage, so the fallback applies only at
# the two call sites that need it (services/tile_server.py's precip-raw and
# river-raw caches), via read_stage_file_with_blob_fallback below.
#
# Blob reads go directly to Azure (ADLSDataStore, the same class used for
# IMPACT_DATA_STORE='BLOB' mode), not through the AOTS_ANALYSIS_BLOB Snowflake
# stage: Snowflake's GET/PUT file-transfer commands are rejected on any
# EXTERNAL stage ("091003: GET and PUT commands are not supported with
# external stage"). Going straight to Blob also involves no warehouse at all.
_MISSING_PATH_TTL_S = 300
_missing_paths_lock = threading.Lock()
_missing_paths: dict[str, float] = {}

_blob_store_lock = threading.Lock()
_blob_store = None


def _get_blob_store():
    global _blob_store
    if _blob_store is None:
        with _blob_store_lock:
            if _blob_store is None:
                from gigaspatial.core.io.adls_data_store import ADLSDataStore
                # Same relative path layout as the Snowflake stage (both are
                # rooted at "met/...", "geodb/...", etc.); stage_path needs
                # no transformation between the two.
                _blob_store = ADLSDataStore(
                    container=app_config.ADLS_CONTAINER_NAME,
                    account_url=app_config.ADLS_ACCOUNT_URL,
                    sas_token=app_config.ADLS_SAS_TOKEN,
                )
    return _blob_store


def _is_snowflake_missing_file_error(err) -> bool:
    """True only for Snowflake's own real "file does not exist" GET failure
    (error code 253006: "While getting file(s) there was an error: the file
    does not exist."), NOT a stage-level misconfiguration/permission error
    like "Stage '...' does not exist or not authorized" (002003); that
    error also contains the bare phrase "does not exist" but means a real
    operational failure (dropped/renamed/de-authorized stage), not real data
    absence, and must not be treated as "retry Blob, maybe negative-cache."
    A bare substring check alone misclassifies that case, so this requires
    the specific error code alongside it."""
    s = str(err)
    return '253006' in s and 'does not exist' in s.lower()


def _is_blob_missing_error(err) -> bool:
    """True for a genuine missing-blob failure (BlobNotFound). Checked via
    both the stable Azure error code and the free-text phrase, for the same
    defense-in-depth reason as _is_snowflake_missing_file_error above:
    neither substring appears for a bad SAS token, a wrong container, or a
    bad account host, only for a real 404."""
    s = str(err).lower()
    return 'blobnotfound' in s or 'does not exist' in s


def read_stage_file_with_blob_fallback(stage_path: str):
    """Read `stage_path` from Azure Blob first, falling back to the app's
    configured (internal) Snowflake stage when Blob doesn't have it
    (see the module comment above this function for the reasoning).

    Meaningless outside IMPACT_DATA_STORE='SNOWFLAKE' (the stage-duality gap
    this solves is Snowflake-stage-specific, and in BLOB mode
    get_data_store() already IS the Blob store): under LOCAL/BLOB this is a
    plain passthrough to get_data_store().read_file(), no fallback, no
    negative cache.
    """
    if app_config.IMPACT_DATA_STORE != 'SNOWFLAKE':
        return get_data_store().read_file(stage_path)

    now = time.time()
    with _missing_paths_lock:
        missed_at = _missing_paths.get(stage_path)
    if missed_at is not None and (now - missed_at) < _MISSING_PATH_TTL_S:
        raise IOError(
            f"{stage_path}: missing from both Azure Blob and the "
            f"{app_config.SNOWFLAKE_STAGE_NAME} Snowflake stage within the last "
            f"{_MISSING_PATH_TTL_S}s (cached, not re-checked)"
        )

    try:
        return _get_blob_store().read_file(stage_path)
    except (IOError, OSError) as blob_err:
        if not _is_blob_missing_error(blob_err):
            raise
        logger.warning(
            "%s: not found in Azure Blob, retrying against the %s Snowflake stage",
            stage_path, app_config.SNOWFLAKE_STAGE_NAME,
        )
        try:
            return get_data_store().read_file(stage_path)
        except (IOError, OSError) as stage_err:
            if not _is_snowflake_missing_file_error(stage_err):
                raise
            with _missing_paths_lock:
                _missing_paths[stage_path] = now
            raise IOError(
                f"{stage_path}: missing from both Azure Blob and the "
                f"{app_config.SNOWFLAKE_STAGE_NAME} Snowflake stage"
            ) from stage_err


_absent_paths_lock = threading.Lock()
_absent_paths: dict[str, float] = {}


def read_blob_file_if_exists(path: str):
    """Bytes of `path` from Azure Blob (the Blob store under SNOWFLAKE,
    otherwise the configured store), or None when that exact object does not exist. Unlike
    read_stage_file_with_blob_fallback there is no internal-stage retry: this is
    for an optional companion file whose absence is a normal state (an
    absence is remembered for _MISSING_PATH_TTL_S so repeated
    requests do not re-probe Blob each time). Any other failure (network,
    credentials) raises."""
    now = time.time()
    with _absent_paths_lock:
        absent_at = _absent_paths.get(path)
    if absent_at is not None and (now - absent_at) < _MISSING_PATH_TTL_S:
        return None
    if app_config.IMPACT_DATA_STORE == 'SNOWFLAKE':
        store = _get_blob_store()
    else:
        store = get_data_store()
    try:
        return store.read_file(path)
    except (IOError, OSError) as err:
        if not (_is_blob_missing_error(err) or isinstance(err, FileNotFoundError)
                or 'no such file' in str(err).lower()):
            raise
        with _absent_paths_lock:
            _absent_paths[path] = now
        return None

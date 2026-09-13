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
    from data_store_utils import get_data_store
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

    This matters because several callers (e.g. page modules) call
    `get_data_store()` unconditionally at *module import* time, regardless
    of `IMPACT_DATA_SOURCE`. Simply moving the `gigaspatial` import inside
    `get_data_store()` does not, by itself, defer anything for those
    callers: the import would still run the moment the module-level
    `get_data_store()` call executes. Wrapping the real store behind this
    proxy means the import genuinely only happens if/when a `.file_exists(
    )`/`.read_file()`/`.open()`/etc call is actually made on it, which
    IMPACT_DATA_SOURCE=SQL callers never do (they either short-circuit past
    the store entirely, e.g. `IMPACT_DATA_SOURCE == 'SQL' or giga_store.
    file_exists(...)`, or never reference the store at all).
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
    # GigaSpatial imports pull in geemap/hdx/sklearn and are only needed for the
    # STAGE file-store path. Deferred via _LazyDataStore so SQL-mode callers
    # (and any caller that ends up never touching the store) never pay this cost,
    # even though this factory itself typically runs at module-import time.
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


# --- Blob-first read, with a legacy-Snowflake-stage fallback, for the two
# --- global raw hazard layers ---
#
# The TC-ECMWF/GloFAS pipelines' Blob cutover moved new met/ writes (precip
# tp, river ro) to real Azure Blob storage (mirrored into Snowflake only as
# the AOTS_ANALYSIS_BLOB EXTERNAL stage), but this app's configured
# SNOWFLAKE_STAGE_NAME still points at the legacy INTERNAL AOTS_ANALYSIS
# stage. Confirmed via a real LIST comparison that AOTS_ANALYSIS_BLOB is NOT
# a mirror of AOTS_ANALYSIS; it's a strict subset (~51k objects vs
# ~247k), holding only what's been written since each pipeline's own
# cutover. Every new precip-raw/river-raw forecast cycle from the cutover
# onward exists ONLY in Blob, while all historical data (geodb/aos_views,
# eval, project_results, glofas, etc.) still lives only in the legacy
# internal stage, for now. The legacy stage is being wound down, so this
# tries Blob FIRST (the common, growing case) and falls back to the legacy
# stage only for whatever's old enough to predate the cutover: the
# opposite priority of a transitional "primary=legacy, fallback=Blob"
# design, deliberately, since that priority will only get more wrong over
# time as more of the legacy stage's content ages past relevance. Flipping
# SNOWFLAKE_STAGE_NAME globally instead would break every OTHER read of
# anything only in the legacy stage, so this is applied ONLY at the two call
# sites that actually hit the gap (services/tile_server.py's
# precip-raw/river-raw caches), via read_stage_file_with_blob_fallback
# below, not app-wide.
#
# Blob reads go DIRECTLY to Azure (ADLSDataStore, the same class this app
# already uses for IMPACT_DATA_STORE='BLOB' mode), not through the
# AOTS_ANALYSIS_BLOB Snowflake stage: Snowflake's GET/PUT file-transfer
# commands are unconditionally rejected on any EXTERNAL stage regardless of
# warehouse ("091003: GET and PUT commands are not supported with external
# stage", confirmed live); this is a hard Snowflake limitation, not a cost
# tradeoff, so reading through Snowflake at all was never actually an
# option for this file. Going straight to Blob is also strictly cheaper
# than a working Snowflake-stage read would have been: zero warehouse
# involvement, not just a smaller one.
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
    ops incident (dropped/renamed/de-authorized stage), not real data
    absence, and must not be treated as "retry Blob, maybe negative-cache."
    A live reproduction confirmed a bare substring check alone misclassifies
    that case, so this requires the specific error code alongside it."""
    s = str(err)
    return '253006' in s and 'does not exist' in s.lower()


def _is_blob_missing_error(err) -> bool:
    """True for a genuine missing-blob failure (BlobNotFound). Checked via
    both the stable Azure error code and the free-text phrase, for the same
    defense-in-depth reason as _is_snowflake_missing_file_error above:
    live reproductions of a bad SAS token, a wrong container, and a bad
    account host all confirmed neither substring ever appears for those
    failures, only for a real 404."""
    s = str(err).lower()
    return 'blobnotfound' in s or 'does not exist' in s


def read_stage_file_with_blob_fallback(stage_path: str):
    """Read `stage_path` from Azure Blob first, falling back to the app's
    configured (legacy internal) Snowflake stage when Blob doesn't have it
    (see the module comment above this function for the full "why", including
    why Blob is tried FIRST rather than as the fallback).

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
            f"{stage_path}: confirmed missing from both Azure Blob and the legacy "
            f"{app_config.SNOWFLAKE_STAGE_NAME} Snowflake stage within the last "
            f"{_MISSING_PATH_TTL_S}s (cached, not re-checked)"
        )

    try:
        return _get_blob_store().read_file(stage_path)
    except (IOError, OSError) as blob_err:
        if not _is_blob_missing_error(blob_err):
            raise
        logger.warning(
            "%s: not found in Azure Blob, retrying against the legacy %s Snowflake stage",
            stage_path, app_config.SNOWFLAKE_STAGE_NAME,
        )
        try:
            return get_data_store().read_file(stage_path)
        except (IOError, OSError) as legacy_err:
            if not _is_snowflake_missing_file_error(legacy_err):
                raise
            with _missing_paths_lock:
                _missing_paths[stage_path] = now
            raise IOError(
                f"{stage_path}: missing from both Azure Blob and the legacy "
                f"{app_config.SNOWFLAKE_STAGE_NAME} Snowflake stage"
            ) from legacy_err


def get_impact_data(data_type: str, giga_store, filepath: str, **sql_params):
    """
    Load impact data via SQL (MAT tables) or file download (stage), controlled by
    the IMPACT_DATA_SOURCE env var.

    IMPORTANT: IMPACT_DATA_SOURCE=SQL connects directly to Snowflake MAT tables and
    bypasses the file store (giga_store) entirely. It works regardless of IMPACT_DATA_STORE
    (LOCAL/BLOB/SNOWFLAKE), as long as Snowflake credentials are configured, which the app
    always requires for TC_TRACKS and PIPELINE_COUNTRIES anyway.

    Args:
        data_type: One of 'school', 'hc', 'shelter', 'wash', 'tile', 'admin_impact', 'admin_cci', 'tile_cci', 'track'.
        giga_store: Configured data store instance (used for STAGE path only).
        filepath: Path to the file on the data store (used for STAGE path only).
        **sql_params: Keyword args passed to the SQL function when IMPACT_DATA_SOURCE='SQL'.
                      Expected keys: country, storm, forecast_date, wind_threshold,
                      zoom_level (tile only), admin_level (admin only).

    Returns:
        pandas.DataFrame (or GeoDataFrame for data_type='track')
    """
    import time
    _t0 = time.perf_counter()

    def _norm(col):
        """Normalize column names: keep E_ prefix uppercase, lowercase everything else."""
        if col.upper().startswith('E_'):
            return 'E_' + col[2:].lower()
        return col.lower()

    if app_config.IMPACT_DATA_SOURCE == 'SQL':
        from components.data.snowflake_utils import (
            get_school_impacts,
            get_hc_impacts,
            get_shelter_impacts,
            get_wash_impacts,
            get_tile_impacts,
            get_admin_impacts,
            get_admin_cci,
            get_tile_cci,
            get_track_impacts,
        )

        country = sql_params['country']
        storm = sql_params['storm']
        forecast_date = sql_params['forecast_date']

        if data_type == 'school':
            result = get_school_impacts(country, storm, forecast_date, sql_params['wind_threshold'])
        elif data_type == 'hc':
            result = get_hc_impacts(country, storm, forecast_date, sql_params['wind_threshold'])
        elif data_type == 'shelter':
            result = get_shelter_impacts(country, storm, forecast_date, sql_params['wind_threshold'])
        elif data_type == 'wash':
            result = get_wash_impacts(country, storm, forecast_date, sql_params['wind_threshold'])
        elif data_type == 'tile':
            result = get_tile_impacts(country, storm, forecast_date, sql_params['wind_threshold'],
                                      sql_params.get('zoom_level', 14))
        elif data_type == 'admin_impact':
            result = get_admin_impacts(country, storm, forecast_date, sql_params['wind_threshold'],
                                       sql_params.get('admin_level', 1))
        elif data_type == 'admin_cci':
            result = get_admin_cci(country, storm, forecast_date, sql_params.get('admin_level', 1))
        elif data_type == 'tile_cci':
            result = get_tile_cci(country, storm, forecast_date, sql_params.get('zoom_level', 14))
        elif data_type == 'track':
            result = get_track_impacts(country, storm, forecast_date, sql_params['wind_threshold'])
        else:
            raise ValueError(f"Unknown data_type '{data_type}' for SQL path")
        if not result.empty:
            # Normalize: keep E_ prefix uppercase, lowercase everything else
            # Matches the convention in STAGE Parquet/CSV files (e.g. E_population, zone_id)
            result.columns = [_norm(c) for c in result.columns]
        source_label = f"SQL/{data_type}"
    else:
        # STAGE path: original behaviour
        from gigaspatial.core.io.readers import read_dataset
        result = read_dataset(filepath, giga_store)
        source_label = f"STAGE/{filepath}"
        # Normalize column names to match SQL path convention (E_population, tile_id, probability…)
        # so the tooltip can find E_population etc. regardless of how the pipeline wrote them.
        if not result.empty:
            result.columns = [_norm(c) for c in result.columns]

    _elapsed = time.perf_counter() - _t0
    logger.debug("[perf] %s → %d rows in %.2fs", source_label, len(result), _elapsed)
    return result
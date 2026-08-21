// maplibre_tiles.js
// MapLibre GL JS integration for the Ahead of the Storm dashboard.
//
// Architecture: MapLibre (zIndex 0) sits behind Leaflet (zIndex 1, transparent).
// Leaflet handles all user interaction (pan/zoom/hover).
// MapLibre is synced to Leaflet by hooking directly into Leaflet's 'move' event,
// which fires on every drag frame: no Dash callback latency.
//
// Exposes on window:
//   applyTileConfig(config)
//   setTileLayerProp(layerId, sourceLayer, prop, stats)
//   setTileLayerVisibility(layerId, visible)
//   dash_clientside.maplibre.updateTileConfig(config)

// ---------------------------------------------------------------------------
// 1. Leaflet map instance registry
// ---------------------------------------------------------------------------
// The Leaflet map instance is captured via dl.Map eventHandlers (sync_maplibre_on_load/move)
// which store it in window._leaflet_maps['main-map'] and window._aots_leaflet_ready_map.
// No prototype patching needed.

window._leaflet_maps = window._leaflet_maps || {};

// ---------------------------------------------------------------------------
// 1b. Tile-URL-aware source update helper
// ---------------------------------------------------------------------------
// MapLibre's own Source.setTiles() never diffs the new value against the
// old one (verified against the vendored maplibre-gl bundle: it
// unconditionally calls load(), which clears that source's tile cache and
// re-requests every currently-tracked tile from the network), so calling
// it with a byte-identical URL still forces a full visible flush+refetch of
// every hazard's raster+admin sources. Several of this file's own URL
// builders go out of their way to keep the URL string stable across
// unrelated config changes (see applyCombinedHazardLayer's own comment on
// this), specifically so MapLibre keeps serving tiles it already has, but
// that work was silently wasted because setTiles() was still called
// unconditionally on every render, regardless of whether the URL actually
// changed. This wrapper tracks the last URL actually applied per source id
// and skips the no-op call/flush when it's unchanged.
window._aotsLastTileUrl = window._aotsLastTileUrl || {};
function _aotsApplySourceTiles(map, sourceId, url, addOptions) {
    var src = map.getSource(sourceId);
    if (src) {
        if (window._aotsLastTileUrl[sourceId] === url) return;
        src.setTiles([url]);
    } else {
        map.addSource(sourceId, Object.assign({ tiles: [url] }, addOptions));
    }
    window._aotsLastTileUrl[sourceId] = url;
}

// ---------------------------------------------------------------------------
// 2. MapLibre initialisation
// ---------------------------------------------------------------------------

function initMaplibre() {
    var container = document.getElementById('maplibre-container');
    if (!container) return;

    // If we have a stale MapLibre instance whose container is no longer in the DOM
    // (happens when Dash re-renders the page on navigation), destroy it so we can re-init.
    if (window._aots_maplibre) {
        var existingContainer = window._aots_maplibre.getContainer();
        if (!document.body.contains(existingContainer) || existingContainer !== container) {
            window._aots_maplibre.remove();
            window._aots_maplibre = null;
            window._aots_maplibre_ready = false;
        } else {
            return; // healthy instance already bound to this container
        }
    }

    // Basemap is rendered entirely by MapLibre; all Leaflet BaseLayers are opacity=0.
    // Leaflet's LayersControl fires 'baselayerchange' → swapMaplibreBasemap swaps tiles here.
    // Using tileSize:256 for all sources so the zoom-1 offset stays consistent.
    var mapboxToken = container.getAttribute('data-mapbox-token') || '';
    window._aots_mapbox_token = mapboxToken;
    var initialTiles = mapboxToken
        ? ['https://api.mapbox.com/styles/v1/mapbox/light-v11/tiles/256/{z}/{x}/{y}?access_token=' + mapboxToken]
        : ['https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
           'https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
           'https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'];
    var basemapSource = {
        'basemap-source': {
            type: 'raster',
            tiles: initialTiles,
            tileSize: 256,
            attribution: mapboxToken ? '© Mapbox © OpenStreetMap contributors' : '© CARTO © OpenStreetMap contributors',
        }
    };
    var basemapLayer = { id: 'basemap-tiles', type: 'raster', source: 'basemap-source', minzoom: 0, maxzoom: 22 };

    var style = {
        version: 8,
        sources: basemapSource,
        layers: [basemapLayer],
    };

window._aots_maplibre = new maplibregl.Map({
        container: 'maplibre-container',
        style: style,
        center: [0, 20],
        zoom: 2,
        interactive: false,        // Leaflet handles all mouse/touch events
        attributionControl: false,
    });

    // Drives window._aots_map_tiles_loading (see _initGlobalLoadingIndicator
    // below): 'dataloading' fires for every tile/source fetch MapLibre
    // kicks off (raster hazard tiles, MVT vector tiles), 'idle' fires once
    // everything currently requested has actually finished rendering.
    window._aots_maplibre.on('dataloading', function () {
        window._aots_map_tiles_loading = true;
        if (window._aots_checkLoadingIndicator) window._aots_checkLoadingIndicator();
    });
    window._aots_maplibre.on('idle', function () {
        window._aots_map_tiles_loading = false;
        if (window._aots_checkLoadingIndicator) window._aots_checkLoadingIndicator();
    });

    window._aots_maplibre_ready = false;
    window._aots_maplibre.on('load', function () {
        window._aots_maplibre_ready = true;
        console.log('[AoTS] MapLibre map loaded, pending config:', !!window._aots_pending_tile_config);
        _onMaplibreReady();
        if (window._aots_pending_tile_config) {
            applyTileConfig(window._aots_pending_tile_config);
            window._aots_pending_tile_config = null;
        }
        // Global raw layers (precip-raw/river-raw), independent of the
        // country-scoped tile config above, see applyGlobalRawConfig below.
        if (window.applyGlobalRawConfig && window._aots_pending_global_raw_config) {
            window.applyGlobalRawConfig(window._aots_pending_global_raw_config);
            window._aots_pending_global_raw_config = null;
        }
    });
}

// Try immediately on DOMContentLoaded, then poll
document.addEventListener('DOMContentLoaded', initMaplibre);
var _initInterval = setInterval(function () {
    initMaplibre();
    if (window._aots_maplibre) clearInterval(_initInterval);
}, 500);

// Re-init when Dash re-renders the page and replaces maplibre-container in the DOM
(function () {
    var _observer = new MutationObserver(function () {
        if (document.getElementById('maplibre-container')) initMaplibre();
    });
    _observer.observe(document.body, { childList: true, subtree: true });
})();

// ---------------------------------------------------------------------------
// 3. Leaflet → MapLibre real-time sync
// ---------------------------------------------------------------------------
// Hook into Leaflet's 'move' event (fires every frame during drag) so MapLibre
// follows instantly: no Dash callback latency, no 800ms flyTo animation lag.

// MapLibre tileSize:256 sources display at the same visual scale as Leaflet when
// MapLibre zoom = Leaflet zoom - 1. Without this -1 offset the basemap appears
// 2× more zoomed in than Leaflet, making GeoJSON layers (tracks, schools, etc.)
// look completely displaced relative to coastlines / city labels.
function _syncMaplibreToLeaflet(lMap) {
    if (!window._aots_maplibre || !window._aots_maplibre_ready) return;
    var c = lMap.getCenter();
    window._aots_maplibre.jumpTo({ center: [c.lng, c.lat], zoom: lMap.getZoom() - 1 });
}

// ---------------------------------------------------------------------------
// 3b. MapLibre hover tooltips (Leaflet mousemove → queryRenderedFeatures)
// ---------------------------------------------------------------------------
// Leaflet sits on top and owns all mouse events. On each mousemove we translate
// the Leaflet containerPoint (same pixel grid as the MapLibre canvas below) to
// queryRenderedFeatures and show a floating tooltip div.

function _getP(props, name) {
    var v = props[name.toUpperCase()];
    if (v !== undefined && v !== null) return v;
    v = props[name.toLowerCase()];
    if (v !== undefined && v !== null) return v;
    return null;
}

// Browser-side counterpart to pages/map_shell_concept.py's own _t().
// window.AOTS_MAP_I18N is populated once at page load from that file's
// _MAP_TOOLTIP_TRANSLATIONS (ms-map-i18n-store's own clientside bridge),
// covering the vocabulary these tooltip builders need. Falls back to the
// English key itself when untranslated, same "never show a blank tooltip"
// contract _t() has server-side.
function _mapT(key) {
    var dict = window.AOTS_MAP_I18N;
    return (dict && dict[key]) || key;
}

// Shared design tokens for every map-hover tooltip below. Matches the hex
// constants pages/map_shell_concept.py's own Python-side UI uses
// pervasively for the equivalent roles (#16232c primary text, #57707e
// secondary label text, #8ea0ab dimmed/muted text, #eef2f5 divider), so
// body text stays a verifiable match to the page's own palette across all
// 8 tooltip-building functions. Per-feature-type TITLE colors
// (tracks/envelopes/schools/health/shelters/wash/river-raw/precip-raw each
// keep their own distinct hue) are intentionally left as-is: meaningful
// visual differentiation between layers, not an inconsistency.
var _AOTS_TT_LABEL = '#57707e';
var _AOTS_TT_VALUE = '#16232c';
var _AOTS_TT_SUB    = '#8ea0ab';
var _AOTS_TT_DIVIDER = '#eef2f5';

function _fmtN(val) {
    if (val === null || val === undefined) return _mapT('N/A');
    if (typeof val === 'number') return new Intl.NumberFormat('en-US').format(Math.ceil(val));
    return String(val);
}

function _fmtPct(val) {
    if (val === null || val === undefined) return _mapT('N/A');
    return (val * 100).toFixed(1) + '%';
}

function _fmtDec(val) {
    if (val === null || val === undefined || (typeof val === 'number' && isNaN(val))) return _mapT('N/A');
    return Number(val).toFixed(2);
}

function _smodLabel(v) {
    var n = parseInt(Number(v) >= 10 ? Number(v) / 10 : Number(v));
    if (n === 1) return _mapT('Rural');
    if (n === 2) return _mapT('Urban Clusters');
    if (n === 3) return _mapT('Urban Centers');
    return _mapT('N/A');
}

function _buildTileTooltip(feature, perHazardProbs) {
    var p = feature.properties || {};
    var G = function(name) { return _getP(p, name); };
    // Admin layer ids are hazard-suffixed (aots-admin-layer-wind / -gust /
    // -river / -rain) since hazards became independently toggleable layers.
    // Match by prefix rather than an exact id that no longer exists.
    var isAdmin = !!(feature.layer && feature.layer.id && feature.layer.id.indexOf('aots-admin-layer') === 0);

    // Label reflects whichever hazard(s) this feature actually represents,
    // rather than a single hardcoded hazard name. `perHazardProbs`
    // (optional, see _visibleRasterHazards) is passed by the caller
    // whenever more than one hazard is active for this tile, so both the
    // combined figure and each hazard's own individual number render.
    //
    // For the TILE hover path (this function's normal caller,
    // _runNetworkHoverLookup), `p.PROBABILITY` comes straight from the
    // server's /tile-value-combined endpoint, which computes the real
    // per-tile bitmask union, the same methodology the raster itself
    // paints with (_combine_bitmask_aware in tile_server.py).
    //
    // The ADMIN/region hover path gets the same treatment where it
    // applies: with 2+ hazards active in Regions view, `p.PROBABILITY`
    // arrives already unioned on the combined admin MVT feature
    // (_combine_bitmask_aware_admin), read directly by
    // _showAdminTooltipImmediate. _combineHazardTileProps' MAX only runs
    // for the genuinely-still-stacked per-hazard admin case (non-combinable
    // Exposure props): see the combinedInfo branch below, which describes
    // whichever hazard this feature actually is.
    var hazardMatch = feature.layer && feature.layer.id && feature.layer.id.match(/^aots-(?:tiles|admin)-layer-(\w+)/);
    var hazard = hazardMatch ? hazardMatch[1] : 'wind';
    // 'combined' is the hazard key the COMBINED admin layer's own id
    // ('aots-admin-layer-combined') yields through the regex above; it
    // only ever reaches the single-hazard label line below in the
    // degenerate case where exactly one per-hazard probability came back
    // on a combined feature, but a real label beats silently falling back
    // to wind's ("Tropical Cyclone") for a multi-hazard layer.
    var _HAZARD_TT_LABELS = { wind: 'Tropical Cyclone', gust: 'Gust', river: 'River Flooding', rain: 'Rainfall',
                              combined: 'Combined Hazard' };
    var hazardLabel = _mapT(_HAZARD_TT_LABELS[hazard] || _HAZARD_TT_LABELS.wind);

    var prob   = G('PROBABILITY') || 0;
    var pop    = G('POPULATION');
    var inf    = G('INFANT_POPULATION');
    var sch    = G('SCHOOL_AGE_POPULATION');
    var ado    = G('ADOLESCENT_POPULATION');
    var blt    = G('BUILT_SURFACE_M2');
    var smod   = G('SMOD_CLASS');
    var rwi    = G('RWI');
    var modpov = G('MODERATE_POVERTY_PROB');
    var sevpov = G('SEVERE_POVERTY_PROB');

    var n_scl  = G('NUM_SCHOOLS');
    var n_hcs  = G('NUM_HCS');
    var n_shlt = G('NUM_SHELTERS');
    var n_wash = G('NUM_WASH');

    var e_pop  = G('E_POPULATION');
    var e_inf  = G('E_INFANT_POPULATION');
    var e_sch  = G('E_SCHOOL_AGE_POPULATION');
    var e_ado  = G('E_ADOLESCENT_POPULATION');
    var e_blt  = G('E_BUILT_SURFACE_M2');
    var e_scl  = G('E_NUM_SCHOOLS');
    var e_hcs  = G('E_NUM_HCS');
    var e_shlt = G('E_NUM_SHELTERS');
    var e_wash = G('E_NUM_WASH');
    var pin    = G('E_PEOPLE_IN_NEED');
    var chin   = G('E_CHILDREN_IN_NEED');

    var children = (inf !== null || sch !== null || ado !== null)
        ? (inf || 0) + (sch || 0) + (ado || 0) : null;

    var fmtE = function(eVal, base, pr) {
        if (eVal !== null && eVal !== undefined && eVal > 0) {
            return ' <span style="color:#dc143c;font-size:0.88em;">(~' + _fmtN(eVal) + ')</span>';
        }
        if (!pr || pr <= 0 || base === null || base === undefined || base <= 0) return '';
        var exp = base * pr;
        if (exp <= 0) return '';
        return ' <span style="color:#dc143c;font-size:0.88em;">(~' + _fmtN(exp) + ')</span>';
    };

    // Title colors stay per-feature-type (meaningful visual differentiation
    // between admin/tile layers, not an inconsistency); only the BODY text
    // scale below was actually inconsistent (see _AOTS_TT_* tokens' own
    // comment) and gets normalized here.
    var titleColor = isAdmin ? '#2e7d32' : '#4169E1';
    var titleLabel = _mapT(isAdmin ? 'Region Statistics' : 'Tile Statistics');
    var name = G('NAME') || '';

    var _esc = function(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); };
    var html = '<div style="font-size:13px;font-weight:600;color:' + titleColor + ';margin-bottom:3px;">' + titleLabel + '</div>';
    if (name) html += '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';font-weight:500;margin-bottom:4px;">' + _esc(name) + '</div>';

    if (prob > 0) {
        html += '<div style="font-size:11px;color:#dc143c;font-weight:600;margin-top:4px;">' + _mapT('Expected Impact') + ':</div>';
        if (perHazardProbs && perHazardProbs.length > 1) {
            // Multiple hazards active for this tile: show the combined
            // figure first, then each hazard's own real number as a
            // sub-row (same visual convention the age-band rows below
            // already use for "detail under a bold parent").
            //
            // The methodology disclosure used to be a small (ⓘ) info glyph
            // carrying the explanation as a native `title` attribute,
            // removed: a native `title`
            // tooltip nested INSIDE this hover tooltip (itself a
            // mousemove-driven overlay) was in practice unreachable:
            // moving the mouse toward that tiny icon moves/closes the
            // outer tooltip before the native one can appear. The two REAL
            // per-member-union cases below (isCombinedAdmin and the tile
            // path) are now explained, with a concrete worked example, by
            // a reliably-hoverable dmc.Tooltip on the EXPOSURE tab's own
            // "Hazard Probability" radio option (pages/map_shell_concept.py,
            // the dmc.Radio with value="probability"), a STATIC control
            // with no such problem.
            //
            // The MAX-based case (N stacked per-hazard admin layers, which
            // applyTileConfig still uses for non-combinable Exposure props
            // like In Need/RWI/poverty) is methodologically DIFFERENT: a
            // genuine max(), not a real joint measurement, and that
            // control's own tooltip only describes the union case, so this
            // one caveat stays here, but as ALWAYS-VISIBLE text instead of
            // a hidden-behind-a-broken-hover icon, since it's arguably the
            // more important of the two to actually see.
            var isCombinedAdmin = !!(feature.layer && feature.layer.id === 'aots-admin-layer-combined');
            var isMaxBased = isAdmin && !isCombinedAdmin;
            html += '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';font-weight:600;">' + _mapT('Combined') + ' ' + _mapT('Impact Probability') + ': ' + _fmtPct(prob) + '</div>';
            if (isMaxBased) {
                html += '<div style="font-size:9.5px;color:' + _AOTS_TT_SUB + ';font-style:italic;">' + _mapT('Combined = highest individual hazard probability, not a joint measurement.') + '</div>';
            }
            perHazardProbs.forEach(function(hp) {
                var lbl = _mapT(_HAZARD_TT_LABELS[hp.hazard] || hp.hazard);
                html += '<div style="font-size:10px;color:' + _AOTS_TT_SUB + ';padding-left:10px;font-style:italic;">' + lbl + ': ' + _fmtPct(hp.prob) + '</div>';
            });
        } else {
            html += '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + hazardLabel + ' ' + _mapT('Impact Probability') + ': ' + _fmtPct(prob) + '</div>';
        }
        html += '<hr style="margin:5px 0;border:none;border-top:1px solid ' + _AOTS_TT_DIVIDER + ';">';
    }
    if (pin !== null && pin !== undefined && pin > 0) {
        html += '<div style="font-size:11px;color:#f59f00;font-weight:600;margin-top:4px;">' + _mapT('In Need (across all wind speeds)') + ':</div>'
              + '<div style="font-size:11px;color:#f59f00;">' + _mapT('Population') + ': ' + _fmtN(pin) + '</div>';
        if (chin !== null && chin !== undefined && chin > 0) {
            html += '<div style="font-size:11px;color:#f59f00;">' + _mapT('Children (total)') + ': ' + _fmtN(chin) + '</div>';
        }
        html += '<hr style="margin:5px 0;border:none;border-top:1px solid ' + _AOTS_TT_DIVIDER + ';">';
    }

    html += '<div style="font-size:11px;color:' + _AOTS_TT_LABEL + ';margin-top:4px;"><strong>' + _mapT(isAdmin ? 'Region' : 'Tile') + ' ' + _mapT('Base Data') + ':</strong></div>'
          + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Population') + ': ' + _fmtN(pop) + fmtE(e_pop, pop, prob) + '</div>'
          + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Children (total)') + ': ' + (children !== null ? _fmtN(children) : _mapT('N/A')) + fmtE(null, children, prob) + '</div>'
          + '<div style="font-size:10px;color:' + _AOTS_TT_SUB + ';padding-left:10px;font-style:italic;">' + _mapT('Age 0–4') + ': ' + _fmtN(inf) + fmtE(e_inf, inf, prob) + '</div>'
          + '<div style="font-size:10px;color:' + _AOTS_TT_SUB + ';padding-left:10px;font-style:italic;">' + _mapT('Age 5–14') + ': ' + _fmtN(sch) + fmtE(e_sch, sch, prob) + '</div>'
          + '<div style="font-size:10px;color:' + _AOTS_TT_SUB + ';padding-left:10px;font-style:italic;">' + _mapT('Age 15–19') + ': ' + _fmtN(ado) + fmtE(e_ado, ado, prob) + '</div>'
          + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Schools') + ': ' + _fmtN(n_scl) + fmtE(e_scl, n_scl, prob) + '</div>'
          + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Health Centers') + ': ' + _fmtN(n_hcs) + fmtE(e_hcs, n_hcs, prob) + '</div>'
          + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Shelters') + ': ' + _fmtN(n_shlt) + fmtE(e_shlt, n_shlt, prob) + '</div>'
          + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('WASH Facilities') + ': ' + _fmtN(n_wash) + fmtE(e_wash, n_wash, prob) + '</div>'
          + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Built Surface') + ': ' + (blt && blt > 0 ? _fmtN(blt) + ' m²' + fmtE(e_blt, blt, prob) : _mapT('N/A')) + '</div>'
          + '<hr style="margin:5px 0;border:none;border-top:1px solid ' + _AOTS_TT_DIVIDER + ';">'
          + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Settlement') + ': ' + (smod !== null ? _smodLabel(smod) : _mapT('N/A')) + '</div>'
          + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Wealth Index (RWI)') + ': ' + _fmtDec(rwi) + '</div>'
          + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Moderate Child Poverty') + ': ' + _fmtPct(modpov) + '</div>'
          + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Severe Child Poverty') + ': ' + _fmtPct(sevpov) + '</div>';

    return html;
}

// Builds the hover tooltip for the two GLOBAL, country-independent raw
// hazard rasters (river-extent/precip-rate). _buildTileTooltip above is
// built entirely around the per-country impact-tile schema
// (population/schools/etc.), which these two layers don't have (see
// /tile-value/river-raw and /tile-value/precip-raw in
// services/tile_server.py: {probability, rp_tier} / {mean_mm,
// probability} only), so a separate, simpler tooltip is built here instead
// of forcing them through that schema.
function _buildRawLayerTooltip(rawLayer, props, rawConfig) {
    var _esc = function(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); };
    if (rawLayer === 'river') {
        var rpTier = (props.rp_tier || (rawConfig && rawConfig.rp_tier) || 'rp10').toUpperCase();
        var stepH = props.step_h != null ? props.step_h : (rawConfig && rawConfig.river_step_h != null ? rawConfig.river_step_h : 72);
        // A specific member has a real boolean flooded/not-flooded fact,
        // not a "% of members agree" fraction; river_raw_tile_value
        // returns `flooded` (not `probability`) whenever `member` is
        // passed server-side.
        if (props.member != null) {
            return '<div style="font-size:13px;font-weight:600;color:#00ACC1;margin-bottom:3px;">' + _mapT('River Flooding') + ': ' + _mapT('Member') + ' ' + props.member + '</div>'
                 + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Return period') + ': ' + _esc(rpTier) + ', ' + _mapT('accumulation window') + ': ≤' + stepH + 'h</div>'
                 + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + (props.flooded ? _mapT('Flooded under this member') : _mapT('Not flooded under this member')) + '</div>'
                 + '<div style="font-size:10px;color:' + _AOTS_TT_SUB + ';font-style:italic;margin-top:4px;">' + _mapT('Real GloFAS discharge exceeding the return-period threshold, matched against JRC\'s historical flood-extent map, not a simulated depth/extent for this specific event.') + '</div>';
        }
        var prob = props.probability;
        return '<div style="font-size:13px;font-weight:600;color:#00ACC1;margin-bottom:3px;">' + _mapT('River Flooding') + '</div>'
             + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Return period') + ': ' + _esc(rpTier) + '</div>'
             + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Member agreement') + ': ' + _fmtPct(prob) + '</div>'
             + '<div style="font-size:10px;color:' + _AOTS_TT_SUB + ';font-style:italic;margin-top:4px;">' + _mapT('Real GloFAS discharge exceeding the return-period threshold, matched against JRC\'s historical flood-extent map, not a simulated depth/extent for this specific event.') + '</div>';
    }
    // precip
    var windowH = (rawConfig && rawConfig.window_h != null) ? rawConfig.window_h : 6;
    // A specific member has its own real rate, not the ensemble mean or
    // an exceedance probability across members.
    if (props.member != null) {
        return '<div style="font-size:13px;font-weight:600;color:#3CB34B;margin-bottom:3px;">' + _mapT('Rainfall') + ': ' + _mapT('Member') + ' ' + props.member + '</div>'
             + '<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('This member\'s rate') + ': ' + Number(props.member_mm).toFixed(1) + 'mm / ' + windowH + 'h</div>'
             + '<div style="font-size:10px;color:' + _AOTS_TT_SUB + ';font-style:italic;margin-top:4px;">' + _mapT('Real forecasted precipitation (a flood-risk indicator, not a flood forecast).') + '</div>';
    }
    var lines = ['<div style="font-size:13px;font-weight:600;color:#3CB34B;margin-bottom:3px;">' + _mapT('Rainfall') + '</div>'];
    if (props.mean_mm !== null && props.mean_mm !== undefined) {
        lines.push('<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Ensemble-mean rate') + ': ' + Number(props.mean_mm).toFixed(1) + 'mm / ' + windowH + 'h</div>');
    }
    if (props.probability !== null && props.probability !== undefined) {
        var thresholdMm = (rawConfig && rawConfig.threshold_mm != null) ? rawConfig.threshold_mm : 10.0;
        // "100mm" alone is ambiguous without the accumulation window it's
        // measured over (100mm over 6h is a very different storm than
        // 100mm over 120h): states the same "Xmm / Yh" window the
        // mean-rate line above already includes.
        lines.push('<div style="font-size:11px;color:' + _AOTS_TT_VALUE + ';">' + _mapT('Probability') + ' >' + thresholdMm + 'mm / ' + windowH + 'h: ' + _fmtPct(props.probability) + '</div>');
    }
    lines.push('<div style="font-size:10px;color:' + _AOTS_TT_SUB + ';font-style:italic;margin-top:4px;">' + _mapT('Real forecasted precipitation (a flood-risk indicator, not a flood forecast).') + '</div>');
    return lines.join('');
}

var _aots_tooltip_el = null;

function _getTooltipEl() {
    if (!_aots_tooltip_el) {
        _aots_tooltip_el = document.createElement('div');
        _aots_tooltip_el.style.cssText = [
            'position:fixed',
            'background:rgba(255,255,255,0.97)',
            'border:1px solid #ccc',
            'border-radius:6px',
            'padding:8px 10px',
            'pointer-events:none',
            'z-index:9000',
            'max-width:270px',
            'box-shadow:0 2px 8px rgba(0,0,0,0.18)',
            'display:none',
            'font-family:sans-serif',
            'line-height:1.4',
        ].join(';');
        document.body.appendChild(_aots_tooltip_el);
    }
    return _aots_tooltip_el;
}

// Admin layer ids are hazard-suffixed (aots-admin-layer-wind/-gust/-river/-rain).
// Only pass ids that actually exist in the current style to
// queryRenderedFeatures: MapLibre throws if asked to query an id that isn't
// in the style. queryRenderedFeatures only ever returns features from
// currently-visible layers, so passing every hazard's id here is safe even
// when several hazards are simultaneously toggled on.
function _AOTS_ADMIN_LAYER_IDS(map) {
    var ids = _AOTS_HAZARDS
        .map(function (hz) { return 'aots-admin-layer-' + hz; })
        .filter(function (id) { return !!map.getLayer(id); });
    // The COMBINED admin layer is the one actually rendered whenever 2+
    // hazards are active in Regions view; the per-hazard ids above are
    // all hidden then, so without adding it here the region hover would
    // return nothing in exactly the case this layer exists for. Same
    // "only pass ids that exist in the current style" guard, and
    // queryRenderedFeatures still only ever returns features from VISIBLE
    // layers, so listing it alongside the per-hazard ids is safe in both
    // directions.
    if (map.getLayer(_AOTS_COMBINED_IDS.adminLayer)) ids.push(_AOTS_COMBINED_IDS.adminLayer);
    return ids;
}

// Several hazards can be visible at once (independently toggleable).
// Returns EVERY visible per-country hazard, in a fixed wind > gust > river
// > rain order (for stable display order), so the caller can fetch all of
// them and show each one's own real probability plus a combined figure:
// see _combineHazardTileProps below.
function _visibleRasterHazards(map, config) {
    var out = [];
    for (var i = 0; i < _AOTS_HAZARDS.length; i++) {
        var hz = _AOTS_HAZARDS[i];
        var id = 'aots-tiles-layer-' + hz;
        if (map.getLayer(id) && map.getLayoutProperty(id, 'visibility') === 'visible') {
            out.push(hz);
        }
    }
    // When 2+ hazards combine into ONE raster (Classification mode, or
    // Probability mode with a combinable Exposure prop, see
    // applyTileConfig's own useCombined), every individual per-hazard
    // layer above is hidden in favor of aots-combined-layer, so `out`
    // stays empty from the loop above even though a real raster is on
    // screen. Fall back to the SAME active-hazard membership the combined
    // layer itself renders (mirrors _combinedActiveHazardCount): each
    // hazard's own tile-value is still fetched and combined exactly like
    // the non-combined multi-hazard-stack case.
    if (out.length === 0 && map.getLayer(_AOTS_COMBINED_IDS.tilesLayer) &&
        map.getLayoutProperty(_AOTS_COMBINED_IDS.tilesLayer, 'visibility') === 'visible') {
        for (var j = 0; j < _AOTS_HAZARDS.length; j++) {
            var hz2 = _AOTS_HAZARDS[j];
            if (config[hz2 + '_visible'] && config[hz2 + '_has_data']) out.push(hz2);
        }
    }
    return out;
}

// Base/context fields: identical regardless of which hazard's own query
// returned them (population/facility counts describe who lives in this
// tile, not a hazard-specific quantity), so combining just means "fill in
// from whichever hazard's response happens to have them", not a real
// MAX/SUM decision.
var _AOTS_TT_BASE_FIELDS = [
    'NAME', 'POPULATION', 'INFANT_POPULATION', 'SCHOOL_AGE_POPULATION', 'ADOLESCENT_POPULATION',
    'BUILT_SURFACE_M2', 'SMOD_CLASS', 'RWI', 'MODERATE_POVERTY_PROB', 'SEVERE_POVERTY_PROB',
    'NUM_SCHOOLS', 'NUM_HCS', 'NUM_SHELTERS', 'NUM_WASH',
];
// PROBABILITY + every real "expected impact" (E_*) field: genuinely
// hazard-specific, MAX-combined below (MAX not SUM avoids double-counting
// the same population cell just because two active hazards both threaten
// it). This function (_combineHazardTileProps) is only used by the
// ADMIN/region hover path (_showAdminTooltipImmediate). The TILE hover
// path instead calls the server's /tile-value-combined endpoint directly,
// which computes a true per-tile bitmask union
// (services/tile_server.py's _combine_bitmask_aware[_points]): see
// _buildTileTooltip's own header comment. The admin/region path uses the
// equivalent server-side union (_combine_bitmask_aware_admin +
// /tiles/admin-combined) whenever a combined admin layer exists (2+ active
// hazards in Regions view), read directly by _showAdminTooltipImmediate.
// This function still covers the cases that legitimately stack N separate
// per-hazard admin layers: a non-combinable Exposure prop (In Need /
// settlement / RWI / poverty, see applyTileConfig's own isCombinableProp),
// where no combined layer exists to read from. Its MAX over the four
// facility-count E_NUM_* fields also remains the real behaviour even on
// the combined tile, which deliberately doesn't recompute those either:
// see _fetch_admin_combined_tile's own "remaining limitation" note in
// tile_server.py.
var _AOTS_TT_E_FIELDS = [
    'E_POPULATION', 'E_INFANT_POPULATION', 'E_SCHOOL_AGE_POPULATION', 'E_ADOLESCENT_POPULATION',
    'E_BUILT_SURFACE_M2', 'E_NUM_SCHOOLS', 'E_NUM_HCS', 'E_NUM_SHELTERS', 'E_NUM_WASH',
];

// Combines multiple per-hazard tile-value responses ({hazard, props}
// pairs) into ONE props object (MAX-combined PROBABILITY/E_* fields, base
// fields filled in from whichever hazard has them) plus a perHazardProbs
// list (each hazard's own probability, for the individual breakdown rows
// _buildTileTooltip renders under the combined figure). People/Children In
// Need stays wind-only (no vulnerability pipeline exists for gust/river/
// rain, same as the country-level function's own docstring), only ever
// taken from wind's own result.
function _combineHazardTileProps(hazardResults) {
    var combined = {};
    var perHazardProbs = [];
    hazardResults.forEach(function(r) {
        if (!r || !r.props) return;
        var p = r.props;
        var prob = _getP(p, 'PROBABILITY');
        if (prob !== null && prob !== undefined) perHazardProbs.push({ hazard: r.hazard, prob: prob });

        _AOTS_TT_BASE_FIELDS.forEach(function(f) {
            if (combined[f] === undefined) {
                var v = _getP(p, f);
                if (v !== null) combined[f] = v;
            }
        });

        if (prob !== null && prob !== undefined && (combined.PROBABILITY === undefined || prob > combined.PROBABILITY)) {
            combined.PROBABILITY = prob;
        }
        _AOTS_TT_E_FIELDS.forEach(function(f) {
            var v = _getP(p, f);
            if (v === null || v === undefined) return;
            if (combined[f] === undefined || v > combined[f]) combined[f] = v;
        });

        if (r.hazard === 'wind') {
            var pin = _getP(p, 'E_PEOPLE_IN_NEED');
            var chin = _getP(p, 'E_CHILDREN_IN_NEED');
            if (pin !== null && pin !== undefined) combined.E_PEOPLE_IN_NEED = pin;
            if (chin !== null && chin !== undefined) combined.E_CHILDREN_IN_NEED = chin;
        }
    });
    return { combinedProps: combined, perHazardProbs: perHazardProbs };
}

// Which of the two GLOBAL raw rasters (see _AOTS_GLOBAL_RAW_IDS) are
// currently visible: these are a completely separate rendering system
// from the per-country hazard tiles above (no `config.country` dependency
// at all), so this is checked as its own fallback in the hover handler
// rather than folded into _visibleRasterHazards.
// Returns EVERY visible raw layer (both river and precip can be visible at
// once, e.g. the BAVI demo scenario, see _DEMO_SCENARIOS' own comment) so
// the caller can query all of them and show whichever (or both) actually
// have data at this point. River's flood-extent coverage is sparse (most
// pixels return {} even where precip genuinely has data right there), so a
// single-layer pick would silently miss real data under the cursor.
function _visibleRawLayers(map) {
    var ids = _AOTS_GLOBAL_RAW_IDS;
    var out = [];
    if (map.getLayer(ids.riverLayer) && map.getLayoutProperty(ids.riverLayer, 'visibility') === 'visible') {
        out.push('river');
    }
    if (map.getLayer(ids.precipLayer) && map.getLayoutProperty(ids.precipLayer, 'visibility') === 'visible') {
        out.push('precip');
    }
    return out;
}

function _setupHoverTooltips(lMap) {
    if (lMap._aots_tooltip_attached) return;
    lMap._aots_tooltip_attached = true;

    var el = _getTooltipEl();
    var _pending_request = null;
    var _last_lon = null;
    var _last_lat = null;
    // The network-lookup half of this handler (raster tile / raw-layer
    // fetches) would otherwise fire on every mousemove tick that clears the
    // tiny 0.001°-movement threshold: for a fast-moving mouse that's
    // easily 10-20+ concurrent fetches/sec if left undebounced.
    // _hoverDebounceTimer defers the actual network lookup until the mouse
    // has been briefly still (80ms), long enough to feel instant to a
    // human hovering, short enough to filter out fetches for positions the
    // cursor has already moved past. The cheap, local, no-network
    // admin-vector-layer check stays fully immediate (no debounce) since
    // queryRenderedFeatures has no network cost at all.
    var _hoverDebounceTimer = null;
    var _HOVER_DEBOUNCE_MS = 80;

    var _positionTooltipEl = function(clientX, clientY) {
        var x = clientX + 16;
        var y = clientY - 10;
        var w = el.offsetWidth || 270;
        var h = el.offsetHeight || 220;
        if (x + w > window.innerWidth - 10) x = clientX - w - 10;
        if (y + h > window.innerHeight - 10) y = window.innerHeight - h - 10;
        el.style.left = x + 'px';
        el.style.top  = y + 'px';
    };

    // Admin vector-layer check: several hazards' own admin layers
    // (aots-admin-layer-{hazard}) can render a feature at the SAME queried
    // point simultaneously, same real multi-hazard combine as the network
    // tile-value path above, just synchronous/local (no fetch needed,
    // queryRenderedFeatures already returns each hazard's own properties).
    var _showAdminTooltipImmediate = function(adminFeatures, clientX, clientY) {
        // When the COMBINED admin layer is what's rendered (2+ active
        // hazards AND view_mode === 'admin'), its own MVT feature already
        // carries a server-computed per-region, per-member union in
        // PROBABILITY (_combine_bitmask_aware_admin in tile_server.py):
        // the same methodology the combined raster paints with. Read it
        // directly instead of deriving a client-side MAX across N separate
        // single-hazard admin layers' properties.
        //
        // The per-hazard MAX path below covers every case this doesn't:
        // exactly one hazard active, view_mode === 'tiles', or a
        // non-combinable Exposure prop (In Need / settlement / RWI /
        // poverty) where applyTileConfig deliberately keeps stacking real
        // per-hazard admin layers.
        var combinedFeature = null;
        for (var ci = 0; ci < adminFeatures.length; ci++) {
            var cf = adminFeatures[ci];
            if (cf.layer && cf.layer.id === _AOTS_COMBINED_IDS.adminLayer) { combinedFeature = cf; break; }
        }
        if (combinedFeature) {
            var cprops = combinedFeature.properties || {};
            // Each active hazard's OWN real marginal probability travels
            // on the same feature (PROBABILITY_WIND/_GUST/_RIVER/_RAIN),
            // so the per-hazard breakdown rows still render with zero
            // extra requests. Absent key = that hazard isn't active (or
            // has no row for this region): skipped, never shown as 0.
            var cPerHazard = [];
            _AOTS_HAZARDS.forEach(function (hz) {
                var v = _getP(cprops, 'PROBABILITY_' + hz.toUpperCase());
                if (v !== null && v !== undefined) cPerHazard.push({ hazard: hz, prob: v });
            });
            el.innerHTML = _buildTileTooltip(
                { properties: cprops, layer: { id: _AOTS_COMBINED_IDS.adminLayer } }, cPerHazard);
            el.style.display = 'block';
            _positionTooltipEl(clientX, clientY);
            return;
        }

        var hazardResults = adminFeatures.map(function(f) {
            var m = f.layer && f.layer.id && f.layer.id.match(/^aots-admin-layer-(\w+)/);
            return { hazard: m ? m[1] : 'wind', props: f.properties };
        });
        var combined = _combineHazardTileProps(hazardResults);
        var topHazard = hazardResults.reduce(function(best, h) {
            return (!best || (_getP(h.props, 'PROBABILITY') || 0) > (_getP(best.props, 'PROBABILITY') || 0)) ? h : best;
        }, null).hazard;
        var feature = { properties: combined.combinedProps, layer: { id: 'aots-admin-layer-' + topHazard } };
        el.innerHTML = _buildTileTooltip(feature, combined.perHazardProbs);
        el.style.display = 'block';
        _positionTooltipEl(clientX, clientY);
    };

    // This branch is deliberately left undebounced (no network cost, see
    // its own comment below), but _showAdminTooltipImmediate's
    // el.innerHTML write followed immediately by _positionTooltipEl's
    // el.offsetWidth/offsetHeight read is a write→read layout-thrash
    // pattern, and mousemove can fire well above 60Hz on some
    // trackpads/mice, which would force a synchronous layout recalculation
    // on every tick while hovering an admin region. Coalesced to at most
    // once per animation frame via requestAnimationFrame (still feels
    // instant to a human, caps the DOM/layout cost at the browser's own
    // paint rate instead of the raw input rate).
    var _adminTooltipRAF = null;
    var _adminTooltipPending = null;
    var _flushAdminTooltip = function() {
        _adminTooltipRAF = null;
        if (_adminTooltipPending) {
            var p = _adminTooltipPending;
            _adminTooltipPending = null;
            _showAdminTooltipImmediate(p.adminFeatures, p.clientX, p.clientY);
        }
    };
    var _showAdminTooltip = function(adminFeatures, clientX, clientY) {
        _adminTooltipPending = { adminFeatures: adminFeatures, clientX: clientX, clientY: clientY };
        if (_adminTooltipRAF === null) {
            _adminTooltipRAF = requestAnimationFrame(_flushAdminTooltip);
        }
    };

    // The network-lookup half of the hover handler: deferred via
    // _hoverDebounceTimer below, never called directly from 'mousemove'.
    function _runNetworkHoverLookup(lon, lat, clientX, clientY) {
        var map = window._aots_maplibre;
        var config = window._aots_tile_config;
        var rawConfig = window._aots_global_raw_config;

        // For the raster tile layer, use the API endpoint. Several hazards
        // can be visible simultaneously: query EVERY visible hazard's own
        // tile-value in parallel and combine them (see
        // _combineHazardTileProps). Per-country hazard tiles need a
        // selected country; the two GLOBAL raw rasters (river-raw/precip-raw,
        // see _visibleRawLayers) don't, and are the normal Global-mode
        // state.
        var hoverHazards = (config && config.country) ? _visibleRasterHazards(map, config) : [];
        var rawLayers = hoverHazards.length === 0 ? _visibleRawLayers(map) : [];
        if (hoverHazards.length === 0 && rawLayers.length === 0) {
            el.style.display = 'none';
            return;
        }

        // Cancel previous pending request
        if (_pending_request) { _pending_request._cancelled = true; }

        var req = { _cancelled: false };
        _pending_request = req;

        var base = ((hoverHazards.length > 0 ? config : rawConfig) && (hoverHazards.length > 0 ? config : rawConfig).tile_server_url != null)
            ? (hoverHazards.length > 0 ? config : rawConfig).tile_server_url : 'http://localhost:8001';
        // Same '' -> window.location.origin fallback as applyGlobalRawConfig
        // and its raster-source siblings: see that function's own comment
        // for the full "why" (local dev has no nginx to make '' resolve to
        // the tile server's own port).
        base = base !== '' ? base : window.location.origin;

        if (hoverHazards.length > 0) {
            // ONE request to /tile-value-combined computes the real
            // per-tile bitmask union server-side (the same methodology
            // the raster underneath paints with) instead of a client-side
            // MAX across N parallel /tile-value/ fetches; see that
            // endpoint's own docstring in tile_server.py. Query-param shape
            // mirrors applyCombinedHazardLayer's own raster-combined URL
            // builder exactly (same config fields, same wire names), so
            // there is one source of truth for "how to ask the server for
            // every active hazard at once" across both the paint path and
            // the hover path.
            var url = base + '/tile-value-combined/'
                + encodeURIComponent(config.country) + '/'
                + encodeURIComponent(config.storm || 'NONE')
                + '?lon=' + lon.toFixed(6)
                + '&lat=' + lat.toFixed(6)
                + '&wind_on=' + (!!config.wind_visible)
                + '&wind_forecast_date=' + encodeURIComponent(config.forecast_date || '')
                + '&wind_threshold=' + (config.wind_threshold != null ? config.wind_threshold : 50)
                + '&gust_on=' + (!!config.gust_visible)
                + (config.gust_threshold != null ? '&gust_threshold=' + config.gust_threshold : '')
                + '&river_on=' + (!!config.river_visible)
                + '&river_forecast_date=' + encodeURIComponent(config.river_forecast_date || '')
                + (config.rp_tier ? '&rp_tier=' + encodeURIComponent(config.rp_tier) : '')
                + (config.river_window != null ? '&river_window=' + config.river_window : '')
                + '&rain_on=' + (!!config.rain_visible)
                + '&rain_forecast_date=' + encodeURIComponent(config.rain_forecast_date || '')
                + (config.threshold_mm != null ? '&threshold_mm=' + config.threshold_mm : '')
                + (config.window_h != null ? '&window_h=' + config.window_h : '');
            fetch(url).then(function(r) { return r.json(); }).then(function(result) {
                if (req._cancelled) return;
                var combinedProps = result && result.combinedProps;
                var perHazardProbs = (result && result.perHazardProbs) || [];
                if (!combinedProps || Object.keys(combinedProps).length === 0) {
                    el.style.display = 'none';
                    return;
                }
                // Layer id tags it with the SINGLE highest-probability
                // hazard (for the single-hazard label path / title-color
                // fallback), derived from the server's own perHazardProbs.
                var topHazard = perHazardProbs.reduce(function(best, h) {
                    return (!best || h.prob > best.prob) ? h : best;
                }, null);
                var feature = { properties: combinedProps, layer: { id: 'aots-tiles-layer-' + (topHazard ? topHazard.hazard : 'wind') } };
                el.innerHTML = _buildTileTooltip(feature, perHazardProbs);
                el.style.display = 'block';
                _positionTooltipEl(clientX, clientY);
            }).catch(function() {
                if (req._cancelled) return;
                el.style.display = 'none';
            });
            return;
        }

        // Query EVERY visible raw layer in parallel and combine whichever
        // ones actually have real data at this point, rather than picking
        // one layer up front (river's flood-extent data is sparse, most
        // pixels return {} even where precip genuinely has real data right
        // there).
        var fetches = rawLayers.map(function(layer) {
            var url = null;
            if (layer === 'river' && rawConfig && rawConfig.river_forecast_time) {
                url = base + '/tile-value/river-raw/' + encodeURIComponent(rawConfig.river_forecast_time)
                    + '?lon=' + lon.toFixed(6) + '&lat=' + lat.toFixed(6)
                    + '&rp_tier=' + encodeURIComponent(rawConfig.rp_tier || 'rp10')
                    + '&step_h=' + (rawConfig.river_step_h != null ? rawConfig.river_step_h : 72)
                    // See _buildRawLayerTooltip's own member branch.
                    + (rawConfig.river_member != null ? '&member=' + rawConfig.river_member : '');
            } else if (layer === 'precip' && rawConfig && rawConfig.precip_forecast_time) {
                // mode=rawConfig.rain_mode: the raster only ever paints
                // ONE of mean/probability at a time (see
                // precip_raw_tile_value's own docstring for the full "why"),
                // so the tooltip has to request the same mode the map is
                // actually rendering rather than always both.
                url = base + '/tile-value/precip-raw/' + encodeURIComponent(rawConfig.precip_forecast_time)
                    + '?lon=' + lon.toFixed(6) + '&lat=' + lat.toFixed(6)
                    + '&mode=' + encodeURIComponent(rawConfig.rain_mode || 'mean')
                    + '&window_h=' + (rawConfig.window_h != null ? rawConfig.window_h : 6)
                    + '&threshold_mm=' + (rawConfig.threshold_mm != null ? rawConfig.threshold_mm : 10.0)
                    // See _buildRawLayerTooltip's own member branch.
                    + (rawConfig.precip_member != null ? '&member=' + rawConfig.precip_member : '');
            }
            if (!url) return Promise.resolve(null);
            return fetch(url).then(function(r) { return r.json(); })
                .then(function(props) { return (props && Object.keys(props).length > 0) ? { layer: layer, props: props } : null; })
                .catch(function() { return null; });
        });

        Promise.all(fetches).then(function(results) {
            if (req._cancelled) return;
            var hits = results.filter(Boolean);
            if (hits.length === 0) {
                el.style.display = 'none';
                return;
            }
            el.innerHTML = hits.map(function(hit) { return _buildRawLayerTooltip(hit.layer, hit.props, rawConfig); }).join('<hr style="margin:5px 0;border:none;border-top:1px solid ' + _AOTS_TT_DIVIDER + ';">');
            el.style.display = 'block';
            _positionTooltipEl(clientX, clientY);
        });
    }

    lMap.on('mousemove', function(e) {
        var map = window._aots_maplibre;
        var config = window._aots_tile_config;
        if (!map || !window._aots_maplibre_ready) {
            el.style.display = 'none';
            return;
        }

        var lon = e.latlng.lng;
        var lat = e.latlng.lat;
        var clientX = e.originalEvent.clientX;
        var clientY = e.originalEvent.clientY;

        // Debounce: skip if barely moved
        if (_last_lon !== null && Math.abs(lon - _last_lon) < 0.001 && Math.abs(lat - _last_lat) < 0.001) {
            // Still check the admin layer via queryRenderedFeatures (vector layer, works fine)
            if (config && config.country) {
                var pt = e.containerPoint;
                var adminFeatures = map.queryRenderedFeatures([pt.x, pt.y], { layers: _AOTS_ADMIN_LAYER_IDS(map) });
                if (adminFeatures && adminFeatures.length > 0) {
                    _showAdminTooltip(adminFeatures, clientX, clientY);
                }
            }
            return;
        }
        _last_lon = lon; _last_lat = lat;

        // Check admin vector layer first (queryRenderedFeatures works for
        // vector layers): country-scoped only, same as before. Stays
        // immediate/undebounced: no network cost.
        if (config && config.country) {
            var pt2 = e.containerPoint;
            var adminFeatures2 = map.queryRenderedFeatures([pt2.x, pt2.y], { layers: _AOTS_ADMIN_LAYER_IDS(map) });
            if (adminFeatures2 && adminFeatures2.length > 0) {
                _showAdminTooltip(adminFeatures2, clientX, clientY);
                return;
            }
        }

        if (_hoverDebounceTimer) { clearTimeout(_hoverDebounceTimer); }
        _hoverDebounceTimer = setTimeout(function() {
            _runNetworkHoverLookup(lon, lat, clientX, clientY);
        }, _HOVER_DEBOUNCE_MS);
    });

    lMap.on('mouseout', function() {
        if (_hoverDebounceTimer) { clearTimeout(_hoverDebounceTimer); _hoverDebounceTimer = null; }
        if (_adminTooltipRAF !== null) { cancelAnimationFrame(_adminTooltipRAF); _adminTooltipRAF = null; }
        _adminTooltipPending = null;
        if (_pending_request) { _pending_request._cancelled = true; }
        _getTooltipEl().style.display = 'none';
    });
}

function _setupLeafletSync(lMap) {
    if (lMap._aots_sync_attached) return;
    lMap._aots_sync_attached = true;

    // During Leaflet zoom animation the 'move' event fires many times.
    // Use rAF to deduplicate: at most one MapLibre jumpTo per frame.
    var _raf_pending = false;
    lMap.on('move', function () {
        if (_raf_pending) return;
        _raf_pending = true;
        requestAnimationFrame(function () {
            _syncMaplibreToLeaflet(lMap);
            _raf_pending = false;
        });
    });

    // After Leaflet finishes a zoom animation, do a precise final sync.
    lMap.on('zoomend', function () { _syncMaplibreToLeaflet(lMap); });

    lMap.on('resize', function () {
        if (window._aots_maplibre) window._aots_maplibre.resize();
    });

    _setupHoverTooltips(lMap);
    if (window._aots_maplibre_ready) _syncMaplibreToLeaflet(lMap);

    // Swap MapLibre basemap tiles when user picks a different basemap in LayersControl.
    // All Leaflet BaseLayers are opacity=0; MapLibre renders the actual basemap.
    lMap.on('baselayerchange', function(e) {
        swapMaplibreBasemap(e.name);
    });
}

function swapMaplibreBasemap(name) {
    var map = window._aots_maplibre;
    if (!map || !window._aots_maplibre_ready) return;
    var token = window._aots_mapbox_token || '';
    var tiles;
    if (name === 'Mapbox Light' && token) {
        tiles = ['https://api.mapbox.com/styles/v1/mapbox/light-v11/tiles/256/{z}/{x}/{y}?access_token=' + token];
    } else if (name === 'OpenStreetMap') {
        tiles = ['https://a.tile.openstreetmap.org/{z}/{x}/{y}.png',
                 'https://b.tile.openstreetmap.org/{z}/{x}/{y}.png',
                 'https://c.tile.openstreetmap.org/{z}/{x}/{y}.png'];
    } else if (name === 'CartoDB Light') {
        tiles = ['https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
                 'https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png',
                 'https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'];
    } else if (name === 'CartoDB Dark') {
        tiles = ['https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png',
                 'https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png',
                 'https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}.png'];
    } else if (name === 'Satellite') {
        tiles = ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'];
    } else {
        tiles = token
            ? ['https://api.mapbox.com/styles/v1/mapbox/light-v11/tiles/256/{z}/{x}/{y}?access_token=' + token]
            : ['https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png'];
    }
    map.getSource('basemap-source').setTiles(tiles);
    console.log('[AoTS] swapMaplibreBasemap →', name);
}

// Poll until both the Leaflet map (set by eventHandlers) and MapLibre are ready
var _syncPoll = setInterval(function () {
    var lMap = window._leaflet_maps['main-map'] || window._aots_leaflet_ready_map;
    if (lMap && window._aots_maplibre) {
        if (lMap !== window._leaflet_maps['main-map']) window._leaflet_maps['main-map'] = lMap;
        _setupLeafletSync(lMap);
        clearInterval(_syncPoll);
    }
}, 200);

// Also run whenever MapLibre finishes loading (in case Leaflet was ready first)
function _onMaplibreReady() {
    // Try multiple sources for the Leaflet map instance
    var lMap = (window._leaflet_maps && window._leaflet_maps['main-map'])
               || window._aots_leaflet_ready_map;

    if (lMap) {
        window._leaflet_maps = window._leaflet_maps || {};
        window._leaflet_maps['main-map'] = lMap;
        _setupLeafletSync(lMap);
        _syncMaplibreToLeaflet(lMap);
    } else if (window._aots_initial_viewport) {
        // Fallback: use the viewport stored by the Dash viewport callback
        var vp = window._aots_initial_viewport;
        window._aots_maplibre.jumpTo({ center: vp.center, zoom: vp.zoom });
    }
}

// ---------------------------------------------------------------------------
// 4. Color expression builder
// ---------------------------------------------------------------------------
// Property names in the PBF tiles come from Snowflake's ST_ASMVT output which
// returns UPPERCASE column names. Both casings are tried via coalesce.

// Same real fixed (min, max) constants as services/tile_server.py's own
// _FIXED_SCALE_COLS (see that dict's own comment for the full real-world
// grounding), kept in sync so the Admin/Regions vector-fill path and the
// raster/Tiles path never disagree on what color the SAME real number
// gets. Only 'probability' remains fixed here: the population-family
// props were tried as a fixed scale, then reverted back to a real,
// data-driven per-country/per-cycle range, and their E_* impact/exposure
// siblings were also reverted (see tile_palettes.json's own
// E_population/etc. entries, moved off this mechanism entirely onto
// 'linear' scale).
var _AOTS_FIXED_SCALE_COLS = {
    'probability': [1 / 51, 1.0],
};

function buildColorExpression(prop, stats) {
    var palettes = window._AOTS_PALETTES || {};
    var palette = palettes[prop];
    if (!palette) return 'transparent';

    var colors = palette.colors;
    var n = colors.length;
    var scale = palette.scale;
    var propUp = prop.toUpperCase();

    if (scale === 'smod') {
        return [
            'match',
            ['floor', ['/', ['coalesce', ['get', propUp], ['get', prop], 0], 10]],
            1, colors[0],
            2, colors[1],
            3, colors[2],
            'transparent'
        ];
    }

    if (scale === 'rwi') {
        var rwiStops = [];
        for (var ri = 0; ri < n; ri++) {
            rwiStops.push(-1.0 + (ri / (n - 1)) * 2.0);
            rwiStops.push(colors[ri]);
        }
        return [
            'interpolate', ['linear'],
            ['coalesce', ['get', propUp], ['get', prop], -999],
            -999, 'transparent'
        ].concat(rwiStops);
    }

    var maxV = (palette.fixed_max != null) ? palette.fixed_max : ((stats[prop] || {}).max || 1);
    var minV = (stats[prop] || {}).min || 0;

    if (scale === 'log') {
        if (maxV <= 0) return 'transparent';
        // 'probability' and the population-family props get a FULLY
        // FIXED scale, both endpoints hardcoded absolute constants, not
        // derived from this country/cycle's own real min/max at all --
        // same real constants and reasoning as _get_minmax's own
        // _FIXED_SCALE_COLS branch (see that dict's own comment for the
        // full real-world grounding).
        //
        // Every other log prop (E_NUM_SCHOOLS, BUILT_SURFACE_M2,
        // E_POPULATION, etc.) anchors the floor at the TRUE minimum
        // (minV, or 1 as a safe fallback when the stats payload has no
        // real min), mirroring services/tile_server.py's own
        // _get_minmax log branch exactly. A floor-raise (real min * a
        // small fraction) was tried here to stop a near-zero
        // outlier tile from stretching the whole ramp, but verified
        // against 5 real datasets (see that Python comment
        // for the full numbers) showed it was actively HURTING every
        // one of these count-like columns, collapsing the vast majority
        // of real cells into one identical flattest color -- the wide
        // dynamic range it was suppressing is exactly the real signal a
        // log scale exists to show here, not noise.
        var fixedScale = _AOTS_FIXED_SCALE_COLS[prop];
        var effectiveMin = fixedScale ? fixedScale[0] : (minV > 0 ? minV : 1);
        if (fixedScale) maxV = fixedScale[1];
        var logMin = Math.log10(effectiveMin);
        var logMax = Math.log10(maxV);
        if (logMin >= logMax) {
            // Degenerate case: min == max, zero real variance in the data.
            // services/tile_server.py's own _get_minmax() (used by the
            // raster/Tiles view) places a degenerate value at the GEOMETRIC
            // MIDPOINT of a widened log range (min/3 .. max*3) rather than
            // either extreme, for an even palette split that lands at
            // colors[Math.floor((n-1)/2)]. This Admin/Regions vector-fill
            // path never calls that function (it colors via a declarative
            // MapLibre expression, not a server-side numeric recompute), so
            // it can't re-derive the exact widen-and-digitize math here.
            // Hardcoding the equivalent middle color keeps a degenerate
            // value reading as moderate severity in both view modes for
            // identical data, instead of one view falsely alarming
            // (darkest tier) and the other falsely muted.
            return ['case',
                ['<=', ['coalesce', ['get', propUp], ['get', prop], 0], 0], 'transparent',
                colors[Math.floor((n - 1) / 2)]
            ];
        }
        var logStep = (logMax - logMin) / n;
        var logStops = [];
        for (var li = 0; li < n; li++) {
            logStops.push(Math.pow(10, logMin + li * logStep));
            logStops.push(colors[li]);
        }
        // Final stop at maxV so the top color covers all high values
        logStops.push(Math.pow(10, logMax));
        logStops.push(colors[n - 1]);
        return [
            'case',
            ['<=', ['coalesce', ['get', propUp], ['get', prop], 0], 0], 'transparent',
            ['interpolate', ['linear'],
                ['coalesce', ['get', propUp], ['get', prop], 0]
            ].concat(logStops)
        ];
    }

    // Default: linear scale
    // IMPORTANT: do NOT add a hardcoded 0,'transparent' before linStops:
    // linStops already starts at 0, creating a duplicate stop → MapLibre "strictly ascending" error.
    var linStep = maxV / n;
    var linStops = [];
    for (var i = 0; i < n; i++) {
        linStops.push(i * linStep);
        linStops.push(colors[i]);
    }
    // Final stop at maxV
    linStops.push(maxV);
    linStops.push(colors[n - 1]);
    return [
        'case',
        ['<=', ['coalesce', ['get', propUp], ['get', prop], 0], 0], 'transparent',
        ['interpolate', ['linear'],
            ['coalesce', ['get', propUp], ['get', prop], 0]
        ].concat(linStops)
    ];
}

// ---------------------------------------------------------------------------
// 5. Apply tile config (core function)
// ---------------------------------------------------------------------------
// Hazards are independently toggleable layers: each gets its own suffixed
// MapLibre source/layer pair (aots-mercator-wind vs aots-mercator-gust vs
// aots-mercator-river vs aots-mercator-rain, etc.) so any combination can be
// visible on the map at once (e.g. Wind + River together).
var _AOTS_HAZARDS = ['wind', 'gust', 'river', 'rain'];

// `suffix` (optional): distinguishes EXTRA per-storm-group layers (see
// "MULTI-STORM GROUPS" section below) from the primary/default set, so a
// country hit by Storm B (not the primary-resolved Storm A) still gets its
// own real map tiles instead of silently reusing Storm A's. Omit/empty for
// the primary group: produces the exact same ids as before this existed.
function _hazardLayerIds(hazardKey, suffix) {
    var suf = suffix ? ('-' + suffix) : '';
    return {
        mercatorSource: 'aots-mercator-' + hazardKey + suf,
        tilesLayer:     'aots-tiles-layer-' + hazardKey + suf,
        adminSource:    'aots-admin-' + hazardKey + suf,
        adminLayer:     'aots-admin-layer-' + hazardKey + suf,
    };
}

// Per-hazard path-segment + query-string values. River/rain are NOT
// storm-scoped at all (see tile_server.py's MERCATOR_TILE_RIVER_MAT/
// MERCATOR_TILE_PRECIP_MAT comments, keyed by COUNTRY + FORECAST_TIME(+
// RP_TIER / +THRESHOLD_MM+WINDOW_H), no STORM/TRACK_ID column exists for
// them), but every tile-server endpoint still has a {storm} URL path
// segment for structural consistency with wind/gust, so river/rain requests
// fill it with an inert placeholder (config.storm, already a real non-empty
// string whenever a country/storm is resolved, reused rather than adding a
// second required config field only to populate an ignored path segment).
// The real identity for river/rain comes entirely from their own
// forecast_date (river_forecast_date/rain_forecast_date) + hazard-specific
// query params.
function _hazardUrlParts(hazardKey, config) {
    var placeholderStorm = config.storm || 'NONE';
    if (hazardKey === 'gust') {
        return {
            storm: config.storm, forecast_date: config.forecast_date,
            qs: '&hazard=gust&gust_threshold=' + config.gust_threshold,
        };
    }
    if (hazardKey === 'river') {
        // River's own cumulative window (see pages/map_shell_concept.py's
        // _build_hazard_tile_config, "river_window" field for the full
        // "why NOT config.window_h" rationale: that field is Rain's own,
        // genuinely different value/option-set). Reuses the SAME generic
        // window_h query param name server-side (services/tile_server.py's
        // _hazard_variant already treats window_h as hazard-agnostic):
        // only this config FIELD name differs, not the wire param name.
        return {
            storm: placeholderStorm, forecast_date: config.river_forecast_date,
            qs: '&hazard=river&rp_tier=' + encodeURIComponent(config.rp_tier)
                + '&window_h=' + (config.river_window != null ? config.river_window : ''),
        };
    }
    if (hazardKey === 'rain') {
        return {
            storm: placeholderStorm, forecast_date: config.rain_forecast_date,
            qs: '&hazard=rain&threshold_mm=' + config.threshold_mm + '&window_h=' + config.window_h,
        };
    }
    return { storm: config.storm, forecast_date: config.forecast_date, qs: '&hazard=wind' };
}

// `group` (optional, see "MULTI-STORM GROUPS" below): a
// {country, storm, forecast_date, stats, admin_stats, suffix} bundle
// overriding the primary config's own country/storm/forecast_date/stats for
// this one extra layer pair, when a country is affected by a DIFFERENT real
// storm than the one the primary group already resolved (wind/gust only,
// river/rain aren't storm-scoped, see _hazardUrlParts's own comment, so a
// single shared forecast_date/rp_tier/threshold_mm already covers every
// selected country there... except when countries genuinely have different
// river/rain forecast_dates too, not yet handled by groups, same scope
// decision as the docstring in pages/map_shell_concept.py's
// _build_hazard_tile_config: wind/gust groups only for this round).
function applyHazardLayer(map, config, hazardKey, group) {
    var ids           = _hazardLayerIds(hazardKey, group && group.suffix);
    var parts         = group
        ? { storm: group.storm, forecast_date: group.forecast_date, qs: _hazardUrlParts(hazardKey, config).qs }
        : _hazardUrlParts(hazardKey, config);
    var country       = group ? group.country : config.country;
    // This per-hazard MapLibre raster/admin layer, the whole
    // Exposure-driven system (Population/Children/.../Hazard Probability),
    // is hidden for every hazard whenever config.hazard_render_mode ===
    // "raw" (pages/map_shell_concept.py's ms-hazard-render-mode switch, top
    // of the Hazard tab): Wind/Gust show their Leaflet envelope polygons
    // instead (tc_view_as is a consumer of this same mode, see
    // _sync_tc_view_as), River/Rain show the global raw cross-border raster
    // instead (applyGlobalRawConfig, driven by hazard_render_mode too).
    // "classification" mode hides this layer differently (see
    // applyTileConfig's own useCombined branch, which skips this function's
    // per-hazard loop entirely).
    var hazardRenderMode = config.hazard_render_mode || 'probability';
    // Wind is the single fallback carrier for the plain Population/Children/
    // etc base layer whenever nothing real is actively weighting the display
    // (config.wind_base_fallback, see its own docstring in
    // pages/map_shell_concept.py): with every hazard checkbox off, or the
    // HAZARDS eye icon hidden, config.wind_visible alone would be false and
    // this whole layer (population included) would never render. Only
    // applies to the primary (non-group) wind call, never gust/river/rain,
    // matching the same "falls back to wind" convention facility_hazard
    // already uses server-side.
    var windFallback = hazardKey === 'wind' && !group && !!config.wind_base_fallback;
    var visible       = (!!config[hazardKey + '_visible'] || windFallback) && hazardRenderMode !== 'raw';
    var base          = config.tile_server_url != null ? config.tile_server_url : 'http://localhost:8001';
    // Vector tiles are fetched inside a MapLibre Web Worker which cannot resolve relative
    // URLs. Use window.location.origin as fallback when base is '' (SPCS proxy mode).
    var absBase       = base !== '' ? base : window.location.origin;
    var stats         = (group ? group.stats : config['stats_' + hazardKey]) || {};
    var adminStats    = (group ? group.admin_stats : config['admin_stats_' + hazardKey]) || stats;
    var tileProp      = config.tile_prop  || null;
    var adminProp     = config.admin_prop || null;
    var _defaultProp  = 'population';

    // River/rain requests have no real forecast_date to run without: skip
    // entirely (leave any existing layer hidden) rather than firing a request
    // that can only 404/return empty (e.g. river_forecast_date not yet
    // resolved because the country has no river data at all).
    if (!country || !parts.forecast_date) {
        if (map.getLayer(ids.tilesLayer)) map.setLayoutProperty(ids.tilesLayer, 'visibility', 'none');
        if (map.getLayer(ids.adminLayer)) map.setLayoutProperty(ids.adminLayer, 'visibility', 'none');
        return;
    }

    var adminUrl = absBase
        + '/tiles/admin/'
        + encodeURIComponent(country) + '/'
        + encodeURIComponent(parts.storm) + '/'
        + encodeURIComponent(parts.forecast_date)
        + '/{z}/{x}/{y}.pbf'
        + '?wind_threshold=' + (config.wind_threshold != null ? config.wind_threshold : 50)
        + '&admin_level=1'
        + parts.qs;

    var rasterUrl = absBase
        + '/tiles/raster/'
        + encodeURIComponent(country) + '/'
        + encodeURIComponent(parts.storm) + '/'
        + encodeURIComponent(parts.forecast_date)
        + '/' + (tileProp ? tileProp.toUpperCase() : 'POPULATION')
        + '/{z}/{x}/{y}.webp'
        + '?wind_threshold=' + (config.wind_threshold != null ? config.wind_threshold : 50)
        + parts.qs;

    _aotsApplySourceTiles(map, ids.mercatorSource, rasterUrl, {
        type: 'raster',
        tileSize: 256,
        minzoom: 3,
        maxzoom: 14,
    });

    _aotsApplySourceTiles(map, ids.adminSource, adminUrl, {
        type: 'vector', minzoom: 4, maxzoom: 10,
    });

    if (!map.getLayer(ids.tilesLayer)) {
        map.addLayer({
            id: ids.tilesLayer,
            type: 'raster',
            source: ids.mercatorSource,
            layout: { visibility: (tileProp && visible) ? 'visible' : 'none' },
            paint: {
                'raster-opacity': 0.8,
                'raster-fade-duration': 0,
                'raster-resampling': 'nearest',
            }
        });
    }

    if (!map.getLayer(ids.adminLayer)) {
        map.addLayer({
            id: ids.adminLayer,
            type: 'fill',
            source: ids.adminSource,
            'source-layer': 'admin',
            layout: { visibility: 'none' },
            paint: {
                'fill-color': buildColorExpression(adminProp || _defaultProp, adminStats),
                'fill-opacity': 0.6,
                'fill-outline-color': 'rgba(0,0,0,0.2)',
            }
        });
    } else if (adminProp) {
        map.setPaintProperty(ids.adminLayer, 'fill-color', buildColorExpression(adminProp, adminStats));
    }

    // Only show layers if a prop was explicitly selected AND this hazard is
    // visible, gated by the Tiles/Regions view switch (cmdbar-detail in
    // map_shell_concept.py's command bar): Tiles (raster) and Regions
    // (admin polygons) are mutually exclusive, never shown at once.
    var viewMode = config.view_mode || 'tiles';
    if (tileProp && viewMode === 'tiles') setTileLayerProp(ids.tilesLayer, 'tiles', tileProp, stats, hazardKey, group);
    else setTileLayerVisibility(ids.tilesLayer, false);
    if (adminProp && visible && viewMode === 'admin') setTileLayerProp(ids.adminLayer, 'admin', adminProp, adminStats, hazardKey, group);
    else setTileLayerVisibility(ids.adminLayer, false);
}

// ---------------------------------------------------------------------------
// MULTI-STORM GROUPS: when selected countries are hit by genuinely
// DIFFERENT storms on the same date (rare but real), each additional storm
// gets its own suffixed wind/gust source+layer pair so its own country's
// map tiles render correctly instead of the primary group's storm being
// applied everywhere. config.extra_wind_groups /
// config.extra_gust_groups (arrays, possibly absent/empty, the overwhelming
// common case of one shared storm) drive this; the primary/default
// wind/gust layers above are completely unaffected when they're empty.
function _applyExtraHazardGroups(map, config) {
    ['wind', 'gust'].forEach(function (hazardKey) {
        var groups = config['extra_' + hazardKey + '_groups'] || [];
        groups.forEach(function (group, i) {
            group.suffix = 'x' + i;
            applyHazardLayer(map, config, hazardKey, group);
        });
    });
    _pruneStaleExtraHazardGroups(map, config);
}

// Remove any surplus extra-group layers/sources left over from a previous
// render with MORE groups than this one (e.g. a country whose distinct storm
// was just deselected): same-index suffixes are reused across renders, so
// anything from `groups.length` up to the previously recorded count is now
// stale. Called from every render path that can change the group set,
// including applyTileConfig's combined-hazard branch (which doesn't render
// the groups at all), so an orphaned layer+source can never outlive the
// selection that created it.
function _pruneStaleExtraHazardGroups(map, config) {
    ['wind', 'gust'].forEach(function (hazardKey) {
        var groups = (config && config['extra_' + hazardKey + '_groups']) || [];
        var prevCount = (window._aots_extra_group_counts && window._aots_extra_group_counts[hazardKey]) || 0;
        for (var i = groups.length; i < prevCount; i++) {
            var ids = _hazardLayerIds(hazardKey, 'x' + i);
            [ids.tilesLayer, ids.adminLayer].forEach(function (id) {
                if (map.getLayer(id)) map.removeLayer(id);
            });
            [ids.mercatorSource, ids.adminSource].forEach(function (id) {
                if (map.getSource(id)) map.removeSource(id);
            });
        }
        window._aots_extra_group_counts = window._aots_extra_group_counts || {};
        window._aots_extra_group_counts[hazardKey] = groups.length;
    });
}

// Combined-hazard raster (real multi-hazard Probability/Classification),
// see services/tile_server.py's _fetch_combined_raster_tile for the "why":
// one real blended/classified raster instead of stacking N separate
// per-hazard raw layers. ONE MapLibre source/layer, reused across every
// mode: only the URL (and visibility) changes.
// `adminLayer` is deliberately named 'aots-admin-layer-combined' rather
// than the 'aots-combined-…' prefix its two raster siblings use: the
// 'aots-admin-layer' prefix is a real, load-bearing CONTRACT elsewhere in
// this file: _buildTileTooltip detects an admin feature with
// `id.indexOf('aots-admin-layer') === 0` and derives its hazard key with
// /^aots-(?:tiles|admin)-layer-(\w+)/, and _AOTS_ADMIN_LAYER_IDS builds the
// hover query list from that same shape. Naming it consistently with the
// raster siblings instead would silently make the combined region tooltip
// render as a TILE tooltip. The SOURCE id has no such contract, so it keeps
// the 'aots-combined-' prefix.
var _AOTS_COMBINED_IDS = {
    mercatorSource: 'aots-combined-source',
    tilesLayer:     'aots-combined-layer',
    adminSource:    'aots-combined-admin-source',
    adminLayer:     'aots-admin-layer-combined',
};

// Combined-ADMIN color-scale ranges. The per-hazard admin layers each get
// their own config.admin_stats_{hazard} straight from /admin-stats/; the
// combined layer has no such server-side stats endpoint of its own, and
// two DIFFERENT hazards' ranges are not interchangeable, so this derives
// one self-consistent range from the active hazards' own stats, mirroring
// the same logic services/tile_server.py's own combined RASTER uses for
// exactly this problem:
//   - 'probability': min of mins, max = min(1, SUM of maxes). A true
//     union's probability can never exceed the sum of its constituents'
//     (union bound, P(A∪B) <= P(A)+P(B), holds without any independence
//     assumption), looser than the exact max but a real bound, so a
//     genuinely-high combined region can never be wrongly clipped into the
//     top color bucket, and the legend doesn't rescale as you pan.
//   - combinable E_* props: scaled against their RAW column's own range
//     (population, not E_population). Two hazards' own E_* ranges aren't
//     comparable (river's country-wide E_population max can be far smaller
//     than rain's purely because river's highest probability is lower), so
//     mixing them directly would produce a non-monotonic result: a
//     combined value painted a LOWER bucket than either hazard alone,
//     despite combined_E >= max(E_a, E_b) always holding. The raw column
//     is hazard-independent and always >= the combined value, so it can
//     never break monotonicity.
var _AOTS_COMBINED_EXPOSURE_RAW_STAT = {
    'E_population':              'population',
    'E_children_total':          'children_total',
    'E_infant_population':       'infant_population',
    'E_school_age_population':   'school_age_population',
    'E_adolescent_population':   'adolescent_population',
    'E_built_surface_m2':        'built_surface_m2',
};

function _combinedAdminStats(config) {
    var active = _AOTS_HAZARDS.filter(function (hz) {
        return config[hz + '_visible'] && config[hz + '_has_data'];
    });
    var out = {};
    // Base/raw columns (population/children/built-up/RWI/poverty/…) are
    // identical across hazards (they describe who lives in the region,
    // not a hazard-specific quantity), so "first hazard that has it wins"
    // is a real fill-in, not a combine decision.
    active.forEach(function (hz) {
        var s = config['admin_stats_' + hz] || {};
        Object.keys(s).forEach(function (k) { if (out[k] === undefined) out[k] = s[k]; });
    });
    var pmin = null, psum = 0, anyProb = false;
    active.forEach(function (hz) {
        var s = (config['admin_stats_' + hz] || {})['probability'];
        if (!s) return;
        anyProb = true;
        if (s.min != null && (pmin === null || s.min < pmin)) pmin = s.min;
        psum += (s.max || 0);
    });
    if (anyProb) out['probability'] = { min: (pmin !== null ? pmin : 0), max: Math.min(1, psum) };
    Object.keys(_AOTS_COMBINED_EXPOSURE_RAW_STAT).forEach(function (eProp) {
        var rawProp = _AOTS_COMBINED_EXPOSURE_RAW_STAT[eProp];
        if (out[rawProp]) out[eProp] = out[rawProp];
    });
    return out;
}

// Mirrors services/tile_server.py's _COMBINED_EXPOSURE_RAW_COL keys exactly
// (same cross-file duplication convention this repo already has for
// propMap/ePropMap): the only Exposure props with a real raw-count ×
// probability relationship, so the only ones a combined mode="exposure"
// raster can legitimately color.
var _AOTS_COMBINABLE_EXPOSURE_PROPS = [
    'E_POPULATION', 'E_CHILDREN_TOTAL', 'E_INFANT_POPULATION',
    'E_SCHOOL_AGE_POPULATION', 'E_ADOLESCENT_POPULATION', 'E_BUILT_SURFACE_M2',
];

// How many of the 4 hazards are both checked AND have real data behind
// them: same has_data signal any_hazard_on already uses server-side (see
// _build_hazard_tile_config's own wind_has_data/gust_has_data/river_has_
// data/rain_has_data comment), so a checked-but-disabled/no-data checkbox
// never counts as "active" here either.
function _combinedActiveHazardCount(config) {
    return ['wind', 'gust', 'river', 'rain'].reduce(function (n, hz) {
        return n + ((config[hz + '_visible'] && config[hz + '_has_data']) ? 1 : 0);
    }, 0);
}

function _hideCombinedHazardLayer(map) {
    if (map.getLayer(_AOTS_COMBINED_IDS.tilesLayer)) {
        map.setLayoutProperty(_AOTS_COMBINED_IDS.tilesLayer, 'visibility', 'none');
    }
    // The combined ADMIN layer hides alongside the raster everywhere the
    // raster hides (the two early-returns in applyTileConfig, the
    // hazards_hidden branch, and the non-combined per-hazard branch),
    // rather than being left for each caller to remember: every existing
    // call site means "hide the whole combined rendering", not "hide its
    // raster half".
    if (map.getLayer(_AOTS_COMBINED_IDS.adminLayer)) {
        map.setLayoutProperty(_AOTS_COMBINED_IDS.adminLayer, 'visibility', 'none');
    }
}

function applyCombinedHazardLayer(map, config) {
    var ids = _AOTS_COMBINED_IDS;
    var base = config.tile_server_url != null ? config.tile_server_url : 'http://localhost:8001';
    // The combined tile-server endpoint has 3 real modes (probability/
    // classification/exposure, see _fetch_combined_raster_tile): "raw"
    // render mode never routes here at all (see applyTileConfig's own
    // useCombined, which is always false for "raw"). "exposure" (real
    // raw-count × combined-probability expected impact) applies whenever
    // the resolved tile_prop is one of _AOTS_COMBINABLE_EXPOSURE_PROPS
    // (Population/Children/Infant/School-age/Adolescent/Built-up); plain
    // "probability" covers both Classification's own always-visible-hazard
    // gate above it and the Exposure tab's "Hazard Probability" selection
    // itself (tile_prop === 'probability', not in that list).
    var propUpper = (config.tile_prop || '').toUpperCase();
    var mode = config.hazard_render_mode === 'classification' ? 'classification'
        : (_AOTS_COMBINABLE_EXPOSURE_PROPS.indexOf(propUpper) !== -1 ? 'exposure' : 'probability');
    var storm = config.storm || 'NONE';  // same placeholder convention as _hazardUrlParts

    if (!config.country) {
        _hideCombinedHazardLayer(map);
        return;
    }

    // Honours config.view_mode (cmdbar-detail in map_shell_concept.py's
    // command bar) mutually exclusively, same as the single-hazard path
    // (applyHazardLayer): Tiles => combined raster, Regions => combined
    // admin polygons, never both.
    //
    // "classification" is the ONE deliberate exception: it stays on the
    // raster even in Regions view. It colors each cell by WHICH hazard(s)
    // hit it: a per-pixel fact with no honest single-value equivalent for
    // a whole admin region (a region containing one wind-only tile and one
    // flood-only tile has no single true classification), which is why the
    // server rejects mode="classification" for /tiles/admin-combined at
    // all rather than quietly serving something else.
    var viewMode = config.view_mode || 'tiles';
    var adminProp = config.admin_prop || null;
    var useAdmin = viewMode === 'admin' && mode !== 'classification';
    // Vector tiles are fetched inside a MapLibre Web Worker which cannot
    // resolve relative URLs, same absBase fallback applyHazardLayer's own
    // admin source already needs for SPCS proxy mode (base === '').
    var absBase = base !== '' ? base : window.location.origin;

    // ONE shared per-hazard query string for BOTH combined routes: they
    // take an identical param shape by design (see admin_combined_tile's
    // own docstring in tile_server.py), so a new hazard field can never be
    // added to the raster URL and forgotten on the admin one.
    //
    // Only an ON hazard's own params go into the URL. A switched-off hazard
    // contributes nothing to a combined tile, so leaving its threshold/date/
    // window out keeps the tile URL byte-identical while that (still fully
    // interactive) slider moves, which keeps MapLibre serving the tiles it
    // already has instead of refetching a whole viewport for identical
    // bytes. wind_forecast_date is shared by Wind and Gust, so it is sent
    // whenever either is on.
    var windOn = !!config.wind_visible, gustOn = !!config.gust_visible;
    var riverOn = !!config.river_visible, rainOn = !!config.rain_visible;
    var sharedQs = '?wind_on=' + windOn + '&gust_on=' + gustOn
        + '&river_on=' + riverOn + '&rain_on=' + rainOn;
    if (windOn || gustOn) {
        sharedQs += '&wind_forecast_date=' + encodeURIComponent(config.forecast_date || '');
    }
    if (windOn) {
        sharedQs += '&wind_threshold=' + (config.wind_threshold != null ? config.wind_threshold : 50);
    }
    if (gustOn && config.gust_threshold != null) {
        sharedQs += '&gust_threshold=' + config.gust_threshold;
    }
    if (riverOn) {
        sharedQs += '&river_forecast_date=' + encodeURIComponent(config.river_forecast_date || '');
        if (config.rp_tier) sharedQs += '&rp_tier=' + encodeURIComponent(config.rp_tier);
        // River's own cumulative lead-time window, kept separate from
        // window_h (Rain's own, below) for the same reason _hazardUrlParts's
        // own river branch keeps them separate: both hazards can be active
        // at once with genuinely different windows. Omitting it would let
        // the server default (_RIVER_WINDOW_DEFAULT, the full 168h horizon)
        // win over the real ms-river-window selection.
        if (config.river_window != null) sharedQs += '&river_window=' + config.river_window;
    }
    if (rainOn) {
        sharedQs += '&rain_forecast_date=' + encodeURIComponent(config.rain_forecast_date || '');
        if (config.threshold_mm != null) sharedQs += '&threshold_mm=' + config.threshold_mm;
        if (config.window_h != null) sharedQs += '&window_h=' + config.window_h;
    }

    var rasterUrl = absBase
        + '/tiles/raster-combined/'
        + encodeURIComponent(config.country) + '/'
        + encodeURIComponent(storm) + '/'
        + mode
        + '/{z}/{x}/{y}.webp'
        + sharedQs
        // `prop` is raster-only: the server has to decide which single
        // quantity to paint into each pixel. The admin MVT carries EVERY
        // property instead and buildColorExpression picks client-side, so
        // sending it there would be meaningless.
        + (mode === 'exposure' ? '&prop=' + propUpper : '');

    var adminUrl = absBase
        + '/tiles/admin-combined/'
        + encodeURIComponent(config.country) + '/'
        + encodeURIComponent(storm) + '/'
        // "classification" never reaches this URL (useAdmin is false for
        // it, and the server rejects it outright): mapped to the nearest
        // real admin mode purely so the string is always valid if the
        // source is created while classification happens to be selected.
        + (mode === 'exposure' ? 'exposure' : 'probability')
        + '/{z}/{x}/{y}.pbf'
        + sharedQs
        + '&admin_level=1';

    _aotsApplySourceTiles(map, ids.mercatorSource, rasterUrl, {
        type: 'raster', tileSize: 256, minzoom: 3, maxzoom: 14,
    });

    // Same minzoom/maxzoom as applyHazardLayer's own per-hazard admin
    // source: admin polygons are served over the same real zoom band
    // regardless of how many hazards feed them.
    _aotsApplySourceTiles(map, ids.adminSource, adminUrl, {
        type: 'vector', minzoom: 4, maxzoom: 10,
    });

    if (!map.getLayer(ids.tilesLayer)) {
        map.addLayer({
            id: ids.tilesLayer,
            type: 'raster',
            source: ids.mercatorSource,
            layout: { visibility: 'none' },
            paint: { 'raster-opacity': 0.8, 'raster-fade-duration': 0, 'raster-resampling': 'nearest' },
        });
    }

    var adminStats = _combinedAdminStats(config);
    if (!map.getLayer(ids.adminLayer)) {
        map.addLayer({
            id: ids.adminLayer,
            type: 'fill',
            source: ids.adminSource,
            // Matches the MVT layer name _fetch_admin_combined_tile encodes
            // ("admin"), identical to the single-hazard admin tiles: one
            // less thing that can differ between the two paths.
            'source-layer': 'admin',
            layout: { visibility: 'none' },
            paint: {
                'fill-color': buildColorExpression(adminProp || 'population', adminStats),
                'fill-opacity': 0.6,
                'fill-outline-color': 'rgba(0,0,0,0.2)',
            }
        });
    } else if (adminProp) {
        map.setPaintProperty(ids.adminLayer, 'fill-color', buildColorExpression(adminProp, adminStats));
    }

    // Mutually exclusive, exactly like applyHazardLayer's own tail: the
    // admin layer additionally needs a real admin_prop selected (nothing
    // to color by otherwise), same gate the single-hazard path uses.
    map.setLayoutProperty(ids.tilesLayer, 'visibility', useAdmin ? 'none' : 'visible');
    map.setLayoutProperty(ids.adminLayer, 'visibility', (useAdmin && adminProp) ? 'visible' : 'none');
}

// Hides every PER-HAZARD raster/admin layer: the four primary ones AND every
// multi-storm "extra group" ('x0'/'x1'/… suffixed wind/gust) layer the
// current config describes (_applyExtraHazardGroups, see #248): a suffixed
// layer left visible keeps painting and stays hoverable, so every hide path
// has to cover both sets. `config` may be null/undefined here (the "no
// country selected" early-return in applyTileConfig calls this before config
// is validated).
//
// The combined layer is left untouched: callers that mean "hide the whole
// hazard rendering" pair this with _hideCombinedHazardLayer (see
// _hideAllHazardLayers), while applyTileConfig's combined branch uses it
// alone, so nothing paints underneath the combined layer.
function _hidePerHazardLayers(map, config) {
    _AOTS_HAZARDS.forEach(function (hazardKey) {
        var ids = _hazardLayerIds(hazardKey);
        [ids.tilesLayer, ids.adminLayer].forEach(function (id) {
            if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', 'none');
        });
    });
    ['wind', 'gust'].forEach(function (hazardKey) {
        var groups = (config && config['extra_' + hazardKey + '_groups']) || [];
        for (var i = 0; i < groups.length; i++) {
            var ids = _hazardLayerIds(hazardKey, 'x' + i);
            [ids.tilesLayer, ids.adminLayer].forEach(function (id) {
                if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', 'none');
            });
        }
    });
}

function _hideAllHazardLayers(map, config) {
    _hidePerHazardLayers(map, config);
    _hideCombinedHazardLayer(map);
}

function applyTileConfig(config) {
    var map = window._aots_maplibre;
    console.log('[AoTS] applyTileConfig called, ready:', window._aots_maplibre_ready, 'config:', config && config.country);

    if (!map || !window._aots_maplibre_ready) {
        window._aots_pending_tile_config = config;
        console.log('[AoTS] MapLibre not ready, stored as pending config');
        return;
    }

    if (!config || !config.country) {
        _hideAllHazardLayers(map, config);
        return;
    }

    // Must be assigned BEFORE the applyHazardLayer loop below.
    // setTileLayerProp (called from inside applyHazardLayer) doesn't
    // receive `config` as a parameter: it reads window._aots_tile_config
    // directly to compute each layer's visibility
    // (`config[hazardKey + '_visible']`). Assigning it here first means
    // every call this render sees its own fresh config, not the previous
    // render's.
    window._aots_tile_config = config;

    // Clicking the HAZARDS eye icon updates ms-hazards-hidden-store, which
    // is a server Input to _build_hazard_tile_config (see
    // config.hazards_hidden's own comment in pages/map_shell_concept.py):
    // every click round-trips through the server and calls applyTileConfig
    // again via ms-tile-config-store's own Output. This check makes the
    // server-computed config authoritative: hidden stays hidden across
    // any future render, not just until the next config change, while
    // setHazardsHiddenOverride's own direct-hide call still provides
    // instant (pre-round-trip) visual feedback.
    if (config.hazards_hidden) {
        // Must NOT be a blanket _hideAllHazardLayers here: the eye icon
        // means "hide the HAZARD weighting/overlays", not "blank the whole
        // map" -- the plain Population/Children/etc base layer piggybacks
        // on wind's own MapLibre layer (see config.wind_base_fallback's own
        // docstring in pages/map_shell_concept.py) and should stay visible.
        // Gust/river/rain (and any multi-storm extra wind/gust groups) have
        // no "base layer" role, they only ever paint hazard-specific data,
        // so they hide unconditionally same as before. The combined
        // (2+-hazard) layer also always hides here: any_hazard_on is
        // already forced False server-side while hazards_hidden is set, so
        // there is nothing for it to combine.
        _hideCombinedHazardLayer(map);
        ['gust', 'river', 'rain'].forEach(function (hazardKey) {
            var ids = _hazardLayerIds(hazardKey);
            [ids.tilesLayer, ids.adminLayer].forEach(function (id) {
                if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', 'none');
            });
        });
        ['wind', 'gust'].forEach(function (hazardKey) {
            var groups = (config['extra_' + hazardKey + '_groups']) || [];
            for (var i = 0; i < groups.length; i++) {
                var ids = _hazardLayerIds(hazardKey, 'x' + i);
                [ids.tilesLayer, ids.adminLayer].forEach(function (id) {
                    if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', 'none');
                });
            }
        });
        applyHazardLayer(map, config, 'wind');
        return;
    }

    // Combined-hazard branch: driven by ms-hazard-render-mode
    // (pages/map_shell_concept.py's Hazard-tab switch, independent of the
    // Exposure tab's own selection, see _hazard_render_mode_switch's own
    // docstring for the full 3-mode model):
    // - "classification": always the combined classification raster,
    //   regardless of active-hazard count or Exposure's selection.
    // - "probability" (default/neutral): combined whenever 2+ hazards are
    //   simultaneously active AND the currently-resolved Exposure prop has a
    //   real combined value: "probability" itself (real per-tile bitmask
    //   union, _combine_bitmask_aware, no independence formula) or one of
    //   _AOTS_COMBINABLE_EXPOSURE_PROPS (real raw-count × combined-probability
    //   expected impact, see _fetch_combined_raster_tile's own
    //   mode="exposure" docstring in tile_server.py). "In Need"/settlement/
    //   RWI/poverty props have no real combined formula (not a simple
    //   raw×probability product): they fall through to the unchanged
    //   per-hazard loop below.
    // - "raw": never combined here at all; Wind/Gust show Leaflet
    //   envelopes (tc_view_as, unaffected), River/Rain show the separate
    //   GLOBAL raw layer (applyGlobalRawConfig): this branch's job is just
    //   to hide every per-hazard MapLibre raster (see applyHazardLayer's own
    //   hazard_render_mode gate, reused here via the unchanged per-hazard
    //   loop below, no combined layer involved).
    var hazardRenderMode = config.hazard_render_mode || 'probability';
    var propUpper = (config.tile_prop || '').toUpperCase();
    var isCombinableProp = config.tile_prop === 'probability'
        || _AOTS_COMBINABLE_EXPOSURE_PROPS.indexOf(propUpper) !== -1;
    var useCombined = hazardRenderMode === 'classification' || (
        hazardRenderMode === 'probability' && isCombinableProp && _combinedActiveHazardCount(config) >= 2
    );
    if (useCombined) {
        // Every per-hazard layer hides, INCLUDING the multi-storm extra-group
        // ones: the combined layer already accounts for every active hazard,
        // so any per-hazard raster left visible would paint a second 0.8-
        // opacity layer over the same country and stay hoverable. Stale
        // groups are pruned here too, since this branch never renders them.
        _hidePerHazardLayers(map, config);
        _pruneStaleExtraHazardGroups(map, config);
        applyCombinedHazardLayer(map, config);
    } else {
        _hideCombinedHazardLayer(map);
        _AOTS_HAZARDS.forEach(function (hazardKey) {
            applyHazardLayer(map, config, hazardKey);
        });
        _applyExtraHazardGroups(map, config);
    }

    // Sync to current Leaflet viewport (Leaflet is the source of truth for position)
    var lMap = window._leaflet_maps && window._leaflet_maps['main-map'];
    if (lMap) {
        _syncMaplibreToLeaflet(lMap);
    }
}

window.applyTileConfig = applyTileConfig;
// Exposed for pages/map_shell_concept.py's _register_ms_facility_layer:
// facility markers (schools/health/shelters/wash) need the exact same
// per-hazard storm/forecast_date/query-string resolution the raster layers
// use, rather than duplicating river/rain's independent-forecast_date logic
// a second time client-side.
window._hazardUrlParts = _hazardUrlParts;
// Exposed for the same reason: facility markers now also route to the
// combined-hazard endpoint whenever 2+ hazards are active, reusing this
// SAME active-hazard count the raster's own useCombined branch uses,
// rather than re-deriving it a second time client-side.
window._combinedActiveHazardCount = _combinedActiveHazardCount;

// Temporary, purely-visual "hide all hazards" preview (see the HAZARDS label
// click handler in pages/map_shell_concept.py): never touches
// window._aots_tile_config itself, so un-hiding just re-applies it verbatim.
function setHazardsHiddenOverride(hidden) {
    var map = window._aots_maplibre;
    if (!map) return;
    if (hidden) {
        _hideAllHazardLayers(map, window._aots_tile_config);
    } else if (window._aots_tile_config) {
        applyTileConfig(window._aots_tile_config);
    }
}
window.setHazardsHiddenOverride = setHazardsHiddenOverride;

// ---------------------------------------------------------------------------
// 6. Layer toggle helpers
// ---------------------------------------------------------------------------

// `hazardKey`/`group` (optional, passed explicitly by applyHazardLayer):
// falls back to deriving hazardKey from the layerId's own suffix (the
// original behavior, still correct for the PRIMARY/default group's plain
// ids like "aots-tiles-layer-wind") when omitted, for any other caller.
// Deliberately NOT derived this way when a `group` is given: an extra
// group's layer id (e.g. "aots-tiles-layer-wind-x0") has a numbered suffix
// as its last '-'-token, which isn't a real hazard key at all and would
// silently fall back to 'wind' even for a gust group, explicit params
// avoid that ambiguity entirely.
function setTileLayerProp(layerId, sourceLayer, prop, stats, hazardKey, group) {
    var map = window._aots_maplibre;
    if (!map) { console.warn('[AoTS] setTileLayerProp: no map'); return; }

    if (!hazardKey) {
        // Layer ids never contain another '-' inside the hazard key itself,
        // so the last '-'-separated token is always exactly the hazard key,
        // true only for the primary/default (non-grouped) id shape.
        var _idParts = layerId.split('-');
        hazardKey = _idParts[_idParts.length - 1];
        if (_AOTS_HAZARDS.indexOf(hazardKey) === -1) hazardKey = 'wind';
    }

    if (layerId.indexOf('aots-tiles-layer') === 0) {
        // Raster layer: change the tile URL to the new property
        var config = window._aots_tile_config || {};
        var base = config.tile_server_url != null ? config.tile_server_url : 'http://localhost:8001';
        // Same '' -> window.location.origin fallback as applyGlobalRawConfig
        // and its other raster-source siblings: see that function's own
        // comment for the full "why" (local dev has no nginx to make ''
        // resolve to the tile server's own port).
        base = base !== '' ? base : window.location.origin;
        var country = group ? group.country : config.country;
        var parts = group
            ? { storm: group.storm, forecast_date: group.forecast_date, qs: _hazardUrlParts(hazardKey, config).qs }
            : _hazardUrlParts(hazardKey, config);
        var mercatorSource = 'aots-mercator-' + hazardKey + (group ? ('-' + group.suffix) : '');

        if (!country || !prop) return;

        // Map prop name (lowercase underscore) to UPPERCASE column name
        var colMap = {
            'population': 'POPULATION',
            'children_total': 'CHILDREN_TOTAL',
            'infant_population': 'INFANT_POPULATION',
            'school_age_population': 'SCHOOL_AGE_POPULATION',
            'adolescent_population': 'ADOLESCENT_POPULATION',
            'built_surface_m2': 'BUILT_SURFACE_M2',
            'smod_class': 'SMOD_CLASS',
            'rwi': 'RWI',
            'moderate_poverty_prob': 'MODERATE_POVERTY_PROB',
            'severe_poverty_prob': 'SEVERE_POVERTY_PROB',
            'probability': 'PROBABILITY',
            'E_population': 'E_POPULATION',
            'E_children_total': 'E_CHILDREN_TOTAL',
            'E_infant_population': 'E_INFANT_POPULATION',
            'E_school_age_population': 'E_SCHOOL_AGE_POPULATION',
            'E_adolescent_population': 'E_ADOLESCENT_POPULATION',
            'E_built_surface_m2': 'E_BUILT_SURFACE_M2',
            'E_num_schools': 'E_NUM_SCHOOLS',
            'E_num_hcs': 'E_NUM_HCS',
            'E_num_shelters': 'E_NUM_SHELTERS',
            'E_num_wash': 'E_NUM_WASH',
            'E_people_in_need': 'E_PEOPLE_IN_NEED',
            'E_children_in_need': 'E_CHILDREN_IN_NEED',
            'E_infant_in_need': 'E_INFANT_IN_NEED',
            'E_school_age_in_need': 'E_SCHOOL_AGE_IN_NEED',
            'E_adolescent_in_need': 'E_ADOLESCENT_IN_NEED',
            'cci_children': 'CCI_CHILDREN',
            'E_cci_children': 'E_CCI_CHILDREN',
        };
        var colName = colMap[prop] || prop.toUpperCase();

        var newUrl = base
            + '/tiles/raster/'
            + encodeURIComponent(country) + '/'
            + encodeURIComponent(parts.storm) + '/'
            + encodeURIComponent(parts.forecast_date)
            + '/' + colName
            + '/{z}/{x}/{y}.webp'
            + '?wind_threshold=' + (config.wind_threshold != null ? config.wind_threshold : 50)
            + parts.qs;

        if (map.getSource(mercatorSource)) _aotsApplySourceTiles(map, mercatorSource, newUrl);

        // Same hazard_render_mode gate AND wind-fallback OR as applyHazardLayer's
        // own `visible`/`windFallback`: see that function's own comment on
        // this. Must stay in sync, this is the SAME layer's visibility,
        // just recomputed here since setTileLayerProp is also called
        // directly by _register_ms_facility_layer-adjacent code paths that
        // don't go through applyHazardLayer's own `visible` local.
        var windFallback = hazardKey === 'wind' && !group && !!config.wind_base_fallback;
        var hazardVisible = (!!config[hazardKey + '_visible'] || windFallback) && (config.hazard_render_mode || 'probability') !== 'raw';
        if (map.getLayer(layerId)) {
            map.setLayoutProperty(layerId, 'visibility', (prop && hazardVisible) ? 'visible' : 'none');
        }
        console.log('[AoTS] setTileLayerProp (raster)', layerId, prop, '→', colName);
        return;
    }

    // Admin layer: keep existing vector fill-color approach
    if (!map.getLayer(layerId)) {
        console.warn('[AoTS] setTileLayerProp: layer not found:', layerId, ', layers:', map.getStyle() ? map.getStyle().layers.map(function(l){return l.id;}) : 'no style');
        return;
    }
    console.log('[AoTS] setTileLayerProp', layerId, prop);
    map.setPaintProperty(layerId, 'fill-color', buildColorExpression(prop, stats || {}));
    map.setLayoutProperty(layerId, 'visibility', prop ? 'visible' : 'none');
}

function setTileLayerVisibility(layerId, visible) {
    var map = window._aots_maplibre;
    if (!map || !map.getLayer(layerId)) return;
    map.setLayoutProperty(layerId, 'visibility', visible ? 'visible' : 'none');
}

window.setTileLayerProp = setTileLayerProp;
window.setTileLayerVisibility = setTileLayerVisibility;

// ---------------------------------------------------------------------------
// 7. Dash clientside bridge
// ---------------------------------------------------------------------------

window.dash_clientside = window.dash_clientside || {};
window.dash_clientside.maplibre = {
    updateTileConfig: function (config) {
        applyTileConfig(config);
        return window.dash_clientside.no_update;
    },
    updateGlobalRawConfig: function (config) {
        applyGlobalRawConfig(config);
        return window.dash_clientside.no_update;
    }
};

// ---------------------------------------------------------------------------
// 8. Global raw layers: raw precip-rate raster + raw river-discharge raster
// ---------------------------------------------------------------------------
// UNLIKE every hazard in section 5 above (applyTileConfig/applyHazardLayer),
// these two layers are GLOBAL and country/storm-INDEPENDENT: a single
// worldwide tp/dis24 Zarr file covers the whole map for one forecast cycle
// (see services/tile_server.py's own "Global raw precipitation-rate
// endpoints"/"Global raw river-discharge endpoints" sections). They must
// keep rendering with no country selected at all (the default Global view),
// so this is deliberately a fully separate function/config/pending-config
// path from applyTileConfig: NOT folded into _AOTS_HAZARDS or gated by
// config.country the way every other hazard layer is.
//
// Visibility is driven by the ms-river-on ("River Flooding")/ms-rain-on
// ("Rainfall") checkboxes (config.precip_visible/river_visible, resolved by
// _build_global_raw_config in pages/map_shell_concept.py): there are no
// separate raw-layer checkboxes. config.rain_mode ("mean"|"probability",
// from the flood-view-as SegmentedControl nested under Rainfall's own
// controls) selects which server-side aggregation the precip-raw endpoint
// renders. RAIN ONLY. River has no Mean mode: unlike rain, it has no
// second independent quantity, so a Mean toggle there would only describe
// the identical per-cell member-agreement fraction under a different
// name/colour. River always renders Probability, with no mode query param
// at all. Both endpoints are fully pre-colored server-side (fixed
// breakpoint ramps), so there is no client-side color/radius styling
// needed for either layer.
//
// River-raw is a real interpolated raster from the same
// /tiles/raster/{precip-raw,river-raw}/... family as precip-raw, just with
// source id 'river' vs 'precip', not a sparse vector/circle layer.

var _AOTS_GLOBAL_RAW_IDS = {
    precipSource: 'aots-precip-raw-source',
    precipLayer:  'aots-precip-raw-layer',
    riverSource:  'aots-river-raw-source',
    riverLayer:   'aots-river-raw-layer',
};

function applyGlobalRawConfig(config) {
    var map = window._aots_maplibre;
    if (!map || !window._aots_maplibre_ready) {
        window._aots_pending_global_raw_config = config;
        return;
    }
    if (!config) return;
    window._aots_global_raw_config = config;

    var ids  = _AOTS_GLOBAL_RAW_IDS;
    var base = config.tile_server_url != null ? config.tile_server_url : 'http://localhost:8001';
    // Use window.location.origin as fallback when base is '' (SPCS/nginx
    // proxy mode, and equally the local-dev setup: "python app.py on :8050
    // + a separately-run uvicorn tile server on :8001, no nginx"), same
    // fallback its sibling functions above (initMaplibre, applyTileConfig, setTileLayerProp)
    // already apply. Without it, "" resolves as a same-origin relative
    // path, which under local dev silently hits the Dash app's own port
    // (8050) instead of the tile server (8001); Dash's use_pages catch-all
    // then answers with 200 OK + the HTML app shell instead of a 404,
    // which MapLibre tries and fails to decode as a WebP raster tile.
    var absBase = base !== '' ? base : window.location.origin;
    var rainMode = (config.rain_mode === 'probability') ? 'probability' : 'mean';

    // --- Precip-raw raster (radar-style rain-rate / exceedance-probability tiles) ---
    // window_h/threshold_mm (resolved by _build_global_raw_config in
    // pages/map_shell_concept.py from the SAME ms-rain-window/ms-rain-slider
    // controls the country-scoped rain hazard already uses, see
    // _hazardUrlParts's own 'rain' branch above for the identical
    // query-param naming convention) drive the actual raster request rather
    // than the server's hardcoded 6h/10mm default.
    var precipTime = config.precip_forecast_time;
    if (precipTime) {
        var precipWindowH = config.window_h != null ? config.window_h : 6;
        var precipThresholdMm = config.threshold_mm != null ? config.threshold_mm : 10.0;
        // precip_member (resolved by _build_global_raw_config from
        // ensemble-member-select, same control that already filters
        // Wind/Gust tracks/envelopes): when set, the server ignores
        // mode/threshold_mm entirely and renders that one member's own
        // rate instead of the aggregate mean/probability.
        var precipUrl = absBase + '/tiles/raster/precip-raw/' + encodeURIComponent(precipTime)
            + '/{z}/{x}/{y}.webp?mode=' + rainMode
            + '&window_h=' + precipWindowH + '&threshold_mm=' + precipThresholdMm
            + (config.precip_member != null ? '&member=' + config.precip_member : '');
        _aotsApplySourceTiles(map, ids.precipSource, precipUrl, {
            type: 'raster', tileSize: 256, minzoom: 0, maxzoom: 14,
        });
        if (!map.getLayer(ids.precipLayer)) {
            map.addLayer({
                id: ids.precipLayer,
                type: 'raster',
                source: ids.precipSource,
                layout: { visibility: config.precip_visible ? 'visible' : 'none' },
                paint: { 'raster-opacity': 0.75, 'raster-fade-duration': 0, 'raster-resampling': 'nearest' },
            });
        } else {
            map.setLayoutProperty(ids.precipLayer, 'visibility', config.precip_visible ? 'visible' : 'none');
        }
    } else if (map.getLayer(ids.precipLayer)) {
        // No real tp data at all (genuinely empty environment): hide rather
        // than point at a forecast_time that doesn't exist.
        map.setLayoutProperty(ids.precipLayer, 'visibility', 'none');
    }

    // --- River-raw raster (interpolated discharge / exceedance-probability tiles) ---
    // rp_tier (resolved by _build_global_raw_config from ms-river-slider)
    // drives the actual raster request rather than the server's rp10
    // default. No mode param: the endpoint always renders Probability.
    var riverTime = config.river_forecast_time;
    var riverRpTier = config.rp_tier || 'rp10';
    // river_step_h (resolved by _build_global_raw_config from the
    // ms-river-window control) drives the actual raster request rather
    // than the server's hardcoded T+24h default: river flooding is
    // slow-onset (see ms-river-window's own comment in
    // pages/map_shell_concept.py), so a fixed T+24h snapshot has no flood
    // signal for most onboarded countries. Server-side this is a
    // CUMULATIVE window (member-flood union across every day from 24h
    // through this value, not a single-day snapshot, see tile_server.py's
    // own "ACCUMULATION SEMANTICS" comment).
    var riverStepH = config.river_step_h != null ? config.river_step_h : 72;
    if (riverTime) {
        // river_member (same source/rationale as precip_member above):
        // when set, renders that one member's own flood extent (flat
        // single-color mask) instead of the aggregate member-agreement
        // gradient.
        var riverUrl = absBase + '/tiles/raster/river-raw/' + encodeURIComponent(riverTime)
            + '/{z}/{x}/{y}.webp?rp_tier=' + riverRpTier + '&step_h=' + riverStepH
            + (config.river_member != null ? '&member=' + config.river_member : '');
        _aotsApplySourceTiles(map, ids.riverSource, riverUrl, {
            type: 'raster', tileSize: 256, minzoom: 0, maxzoom: 14,
        });
        if (!map.getLayer(ids.riverLayer)) {
            map.addLayer({
                id: ids.riverLayer,
                type: 'raster',
                source: ids.riverSource,
                layout: { visibility: config.river_visible ? 'visible' : 'none' },
                paint: { 'raster-opacity': 0.75, 'raster-fade-duration': 0, 'raster-resampling': 'nearest' },
            });
        } else {
            map.setLayoutProperty(ids.riverLayer, 'visibility', config.river_visible ? 'visible' : 'none');
        }
    } else if (map.getLayer(ids.riverLayer)) {
        // No real dis24 data at all: hide rather than point at a
        // forecast_time that doesn't exist.
        map.setLayoutProperty(ids.riverLayer, 'visibility', 'none');
    }
}

window.applyGlobalRawConfig = applyGlobalRawConfig;

// ---------------------------------------------------------------------------
// Global loading indicator (top bar, next to the country selector, see
// pages/map_shell_concept.py's _ms_loading_badge). dcc.Loading's own
// target_components mechanism does not reliably show a spinner for
// ms-tile-config-store, even nested as a direct child of the Loading
// component. Dash's own generic top-level indicator (a
// `._dash-loading-callback`-classed div Dash inserts directly under
// #react-entry-point while ANY callback is in flight) does reliably appear
// for the same requests, so this reuses that signal via a plain
// MutationObserver instead of relying on target_components.
//
// The Dash-callback signal alone never covers MapLibre's OWN tile network
// activity (raster WebP hazard tiles, MVT vector tiles): those load via
// MapLibre GL's internal networking, kicked off directly from
// applyHazardLayer/setTileLayerProp with no Dash callback (server OR
// clientside) involved at all. That's what a user watching colours paint
// onto the map perceives as "the layers loading", so the badge needs to
// reflect it too, not just Dash's own request/response cycle.
// window._aots_map_tiles_loading is toggled by MapLibre's own
// 'dataloading'/'idle' events (wired up right after the map is constructed
// in initMaplibre above) and OR'd into the same check.
window._aots_map_tiles_loading = false;
function _initGlobalLoadingIndicator() {
    var badge = document.getElementById('ms-global-loading-indicator');
    var mountCheck = document.getElementById('react-entry-point');
    if (!badge || !mountCheck) {
        // Layout not mounted yet on first DOMContentLoaded fire: retry
        // shortly rather than silently giving up (same "poll until ready"
        // pattern initMaplibre already uses above for the map container).
        setTimeout(_initGlobalLoadingIndicator, 200);
        return;
    }
    // Scoped to document.body rather than #react-entry-point alone.
    // dmc.Modal/dmc.Popover/dmc.Tooltip render their content through a
    // Mantine Portal, which by default (no withinPortal=False anywhere on
    // this page's Modals) appends directly to <body> as a SIBLING of
    // #react-entry-point, not a descendant of it. Dash inserts its
    // `_dash-loading-callback` marker on the ancestor of whatever DOM node
    // owns the loading Output: for the Hazard Contribution popup and the
    // Full Impact Breakdown modal, that ancestor is inside the portaled
    // subtree, structurally invisible to a querySelector/observer scoped to
    // #react-entry-point alone. Watching document.body instead covers both
    // #react-entry-point and every Mantine portal mounted alongside it.
    var root = document.body;
    var check = function () {
        var isLoading = !!root.querySelector('._dash-loading-callback')
            || !!window._aots_map_tiles_loading
            || (window._aots_pending_fetches || 0) > 0;
        badge.style.display = isLoading ? '' : 'none';
    };
    // Exposed globally so the MapLibre 'dataloading'/'idle' handlers (which
    // don't mutate the Dash-rendered DOM the MutationObserver below
    // watches) can force an immediate re-check instead of waiting for an
    // unrelated DOM mutation to happen to fire it. Deliberately the real,
    // synchronous `check` (not the coalesced wrapper below): an explicit
    // external caller wants an immediate, accurate answer right now, not a
    // deferred one.
    window._aots_checkLoadingIndicator = check;
    // This MutationObserver watches all of document.body (deliberately,
    // see the comment above re: Mantine portals rendering outside
    // #react-entry-point; do NOT narrow the scope back), so it would
    // otherwise re-fire `check`'s own full-subtree querySelector scan on
    // every single batch of DOM mutations anywhere on the page, including
    // this file's own tooltip repositioning writes (_positionTooltipEl),
    // which compounds with hover activity. Coalesced to at most once per
    // animation frame via requestAnimationFrame, same pattern as
    // _showAdminTooltip's own coalescing above, still reflects real state
    // within one paint frame, just not once per individual mutation record.
    var _loadingCheckRAF = null;
    var _scheduledCheck = function() {
        if (_loadingCheckRAF !== null) return;
        _loadingCheckRAF = requestAnimationFrame(function() {
            _loadingCheckRAF = null;
            check();
        });
    };
    new MutationObserver(_scheduledCheck).observe(root, { attributes: true, childList: true, subtree: true });
    check();
}
document.addEventListener('DOMContentLoaded', _initGlobalLoadingIndicator);
// Also try immediately (same reasoning as initMaplibre above, dash-render
// may have already fired DOMContentLoaded by the time this script runs).
_initGlobalLoadingIndicator();

// maplibre_tiles.js
// MapLibre GL JS integration for the Ahead of the Storm dashboard.
//
// Architecture: MapLibre (zIndex 0) sits behind Leaflet (zIndex 1, transparent).
// Leaflet handles all user interaction (pan/zoom/hover).
// MapLibre is synced to Leaflet by hooking directly into Leaflet's 'move' event,
// which fires on every drag frame — no Dash callback latency.
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
    // below) — 'dataloading' fires for every tile/source fetch MapLibre
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
        // Global raw layers (precip-raw/river-raw) — independent of the
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
// follows instantly — no Dash callback latency, no 800ms flyTo animation lag.

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

function _fmtN(val) {
    if (val === null || val === undefined) return 'N/A';
    if (typeof val === 'number') return new Intl.NumberFormat('en-US').format(Math.ceil(val));
    return String(val);
}

function _fmtPct(val) {
    if (val === null || val === undefined) return 'N/A';
    return (val * 100).toFixed(1) + '%';
}

function _fmtDec(val) {
    if (val === null || val === undefined || (typeof val === 'number' && isNaN(val))) return 'N/A';
    return Number(val).toFixed(2);
}

function _smodLabel(v) {
    var n = parseInt(Number(v) >= 10 ? Number(v) / 10 : Number(v));
    if (n === 1) return 'Rural';
    if (n === 2) return 'Urban Clusters';
    if (n === 3) return 'Urban Centers';
    return 'N/A';
}

function _buildTileTooltip(feature) {
    var p = feature.properties || {};
    var G = function(name) { return _getP(p, name); };
    // Admin layer ids are hazard-suffixed (aots-admin-layer-wind / -gust /
    // -river / -rain) since hazards became independently toggleable layers —
    // match by prefix rather than an exact id that no longer exists.
    var isAdmin = !!(feature.layer && feature.layer.id && feature.layer.id.indexOf('aots-admin-layer') === 0);

    var prob   = G('PROBABILITY') || 0;
    var pop    = G('POPULATION');
    var inf    = G('INFANT_POPULATION');
    var sch    = G('SCHOOL_AGE_POPULATION');
    var ado    = G('ADOLESCENT_POPULATION');
    var blt    = G('BUILT_SURFACE_M2');
    var smod   = G('SMOD_CLASS');
    var rwi    = G('RWI');
    var cci    = G('CCI_CHILDREN');
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

    var titleColor = isAdmin ? '#2e7d32' : '#4169E1';
    var titleLabel = isAdmin ? 'Region Statistics' : 'Tile Statistics';
    var name = G('NAME') || '';

    var _esc = function(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); };
    var html = '<div style="font-size:13px;font-weight:600;color:' + titleColor + ';margin-bottom:3px;">' + titleLabel + '</div>';
    if (name) html += '<div style="font-size:12px;color:#333;font-weight:500;margin-bottom:4px;">' + _esc(name) + '</div>';

    if (prob > 0) {
        html += '<div style="font-size:11px;color:#dc143c;font-weight:600;margin-top:4px;">Expected Impact:</div>'
              + '<div style="font-size:11px;color:#555;">Hurricane Impact Probability: ' + _fmtPct(prob) + '</div>';
        html += '<hr style="margin:5px 0;border:none;border-top:1px solid #ddd;">';
    }
    if (pin !== null && pin !== undefined && pin > 0) {
        html += '<div style="font-size:11px;color:#f59f00;font-weight:600;margin-top:4px;">In Need (across all wind speeds):</div>'
              + '<div style="font-size:11px;color:#f59f00;">Population: ' + _fmtN(pin) + '</div>';
        if (chin !== null && chin !== undefined && chin > 0) {
            html += '<div style="font-size:11px;color:#f59f00;">Children (total): ' + _fmtN(chin) + '</div>';
        }
        html += '<hr style="margin:5px 0;border:none;border-top:1px solid #ddd;">';
    }

    html += '<div style="font-size:11px;color:#777;margin-top:4px;"><strong>' + (isAdmin ? 'Region' : 'Tile') + ' Base Data:</strong></div>'
          + '<div style="font-size:11px;color:#555;">Population: ' + _fmtN(pop) + fmtE(e_pop, pop, prob) + '</div>'
          + '<div style="font-size:11px;color:#555;">Children (total): ' + (children !== null ? _fmtN(children) : 'N/A') + fmtE(null, children, prob) + '</div>'
          + '<div style="font-size:10px;color:#888;padding-left:10px;font-style:italic;">Age 0–4: ' + _fmtN(inf) + fmtE(e_inf, inf, prob) + '</div>'
          + '<div style="font-size:10px;color:#888;padding-left:10px;font-style:italic;">Age 5–14: ' + _fmtN(sch) + fmtE(e_sch, sch, prob) + '</div>'
          + '<div style="font-size:10px;color:#888;padding-left:10px;font-style:italic;">Age 15–19: ' + _fmtN(ado) + fmtE(e_ado, ado, prob) + '</div>'
          + '<div style="font-size:11px;color:#555;">Schools: ' + _fmtN(n_scl) + fmtE(e_scl, n_scl, prob) + '</div>'
          + '<div style="font-size:11px;color:#555;">Health Centers: ' + _fmtN(n_hcs) + fmtE(e_hcs, n_hcs, prob) + '</div>'
          + '<div style="font-size:11px;color:#555;">Shelters: ' + _fmtN(n_shlt) + fmtE(e_shlt, n_shlt, prob) + '</div>'
          + '<div style="font-size:11px;color:#555;">WASH Facilities: ' + _fmtN(n_wash) + fmtE(e_wash, n_wash, prob) + '</div>'
          + '<div style="font-size:11px;color:#555;">Built Surface: ' + (blt && blt > 0 ? _fmtN(blt) + ' m²' + fmtE(e_blt, blt, prob) : 'N/A') + '</div>'
          + '<hr style="margin:5px 0;border:none;border-top:1px solid #ddd;">'
          + '<div style="font-size:11px;color:#555;">CCI: ' + _fmtDec(cci) + '</div>'
          + '<div style="font-size:11px;color:#555;">Settlement: ' + (smod !== null ? _smodLabel(smod) : 'N/A') + '</div>'
          + '<div style="font-size:11px;color:#555;">Wealth Index (RWI): ' + _fmtDec(rwi) + '</div>'
          + '<div style="font-size:11px;color:#555;">Moderate Child Poverty: ' + _fmtPct(modpov) + '</div>'
          + '<div style="font-size:11px;color:#555;">Severe Child Poverty: ' + _fmtPct(sevpov) + '</div>';

    return html;
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
// queryRenderedFeatures — MapLibre throws if asked to query an id that isn't
// in the style. queryRenderedFeatures only ever returns features from
// currently-visible layers, so passing every hazard's id here is safe even
// when several hazards are simultaneously toggled on.
function _AOTS_ADMIN_LAYER_IDS(map) {
    return _AOTS_HAZARDS
        .map(function (hz) { return 'aots-admin-layer-' + hz; })
        .filter(function (id) { return !!map.getLayer(id); });
}

// Several hazards can be visible at once (independently toggleable), but the
// /tile-value raster hover lookup below is a single fetch — pick the first
// visible hazard in this fixed priority order (wind > gust > river > rain)
// deterministically rather than guessing "the" hazard from a single ambient
// config field that no longer exists post-redesign.
function _firstVisibleRasterHazard(map, config) {
    for (var i = 0; i < _AOTS_HAZARDS.length; i++) {
        var hz = _AOTS_HAZARDS[i];
        var id = 'aots-tiles-layer-' + hz;
        if (map.getLayer(id) && map.getLayoutProperty(id, 'visibility') === 'visible') {
            return hz;
        }
    }
    return null;
}

function _setupHoverTooltips(lMap) {
    if (lMap._aots_tooltip_attached) return;
    lMap._aots_tooltip_attached = true;

    var el = _getTooltipEl();
    var _pending_request = null;
    var _last_lon = null;
    var _last_lat = null;

    lMap.on('mousemove', function(e) {
        var map = window._aots_maplibre;
        var config = window._aots_tile_config;
        if (!map || !window._aots_maplibre_ready || !config || !config.country) {
            el.style.display = 'none';
            return;
        }

        var lon = e.latlng.lng;
        var lat = e.latlng.lat;

        // Debounce: skip if barely moved
        if (_last_lon !== null && Math.abs(lon - _last_lon) < 0.001 && Math.abs(lat - _last_lat) < 0.001) {
            // Still check the admin layer via queryRenderedFeatures (vector layer, works fine)
            var pt = e.containerPoint;
            var adminFeatures = map.queryRenderedFeatures([pt.x, pt.y], { layers: _AOTS_ADMIN_LAYER_IDS(map) });
            if (adminFeatures && adminFeatures.length > 0) {
                el.innerHTML = _buildTileTooltip(adminFeatures[0]);
                el.style.display = 'block';
                var x = e.originalEvent.clientX + 16;
                var y = e.originalEvent.clientY - 10;
                var w = el.offsetWidth || 270;
                var h = el.offsetHeight || 220;
                if (x + w > window.innerWidth - 10) x = e.originalEvent.clientX - w - 10;
                if (y + h > window.innerHeight - 10) y = window.innerHeight - h - 10;
                el.style.left = x + 'px';
                el.style.top  = y + 'px';
            }
            return;
        }
        _last_lon = lon; _last_lat = lat;

        // Check admin vector layer first (queryRenderedFeatures works for vector layers)
        var pt = e.containerPoint;
        var adminFeatures = map.queryRenderedFeatures([pt.x, pt.y], { layers: _AOTS_ADMIN_LAYER_IDS(map) });
        if (adminFeatures && adminFeatures.length > 0) {
            el.innerHTML = _buildTileTooltip(adminFeatures[0]);
            el.style.display = 'block';
            var x = e.originalEvent.clientX + 16;
            var y = e.originalEvent.clientY - 10;
            var w = el.offsetWidth || 270;
            var h = el.offsetHeight || 220;
            if (x + w > window.innerWidth - 10) x = e.originalEvent.clientX - w - 10;
            if (y + h > window.innerHeight - 10) y = window.innerHeight - h - 10;
            el.style.left = x + 'px';
            el.style.top  = y + 'px';
            return;
        }

        // For the raster tile layer, use the API endpoint. Several hazards can be
        // visible simultaneously — pick the first one actually showing a raster
        // layer right now (fixed wind > gust > river > rain priority, see
        // _firstVisibleRasterHazard's own comment).
        var hoverHazard = _firstVisibleRasterHazard(map, config);
        if (!hoverHazard) {
            el.style.display = 'none';
            return;
        }

        // Cancel previous pending request
        if (_pending_request) { _pending_request._cancelled = true; }

        var req = { _cancelled: false };
        _pending_request = req;

        var hoverParts = _hazardUrlParts(hoverHazard, config);
        var tilesLayerId = 'aots-tiles-layer-' + hoverHazard;
        var url = (config.tile_server_url != null ? config.tile_server_url : 'http://localhost:8001')
            + '/tile-value/'
            + encodeURIComponent(config.country) + '/'
            + encodeURIComponent(hoverParts.storm) + '/'
            + encodeURIComponent(hoverParts.forecast_date)
            + '?lon=' + lon.toFixed(6)
            + '&lat=' + lat.toFixed(6)
            + '&wind_threshold=' + (config.wind_threshold != null ? config.wind_threshold : 50)
            + hoverParts.qs;

        fetch(url)
            .then(function(r) { return r.json(); })
            .then(function(props) {
                if (req._cancelled) return;
                if (!props || Object.keys(props).length === 0) {
                    el.style.display = 'none';
                    return;
                }
                // Build a fake feature object compatible with _buildTileTooltip
                var feature = { properties: props, layer: { id: tilesLayerId } };
                el.innerHTML = _buildTileTooltip(feature);
                el.style.display = 'block';
                var x = e.originalEvent.clientX + 16;
                var y = e.originalEvent.clientY - 10;
                var w = el.offsetWidth || 270;
                var h = el.offsetHeight || 220;
                if (x + w > window.innerWidth - 10) x = e.originalEvent.clientX - w - 10;
                if (y + h > window.innerHeight - 10) y = window.innerHeight - h - 10;
                el.style.left = x + 'px';
                el.style.top  = y + 'px';
            })
            .catch(function() { el.style.display = 'none'; });
    });

    lMap.on('mouseout', function() {
        _getTooltipEl().style.display = 'none';
    });
}

function _setupLeafletSync(lMap) {
    if (lMap._aots_sync_attached) return;
    lMap._aots_sync_attached = true;

    // During Leaflet zoom animation the 'move' event fires many times.
    // Use rAF to deduplicate — at most one MapLibre jumpTo per frame.
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
        // Floor at 1 when stats report min=0 (zero-value tiles are filtered transparent by the case guard)
        var effectiveMin = (minV > 0) ? minV : 1;
        var logMin = Math.log10(effectiveMin);
        var logMax = Math.log10(maxV);
        if (logMin >= logMax) {
            return ['case',
                ['<=', ['coalesce', ['get', propUp], ['get', prop], 0], 0], 'transparent',
                colors[n - 1]
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
    // IMPORTANT: do NOT add a hardcoded 0,'transparent' before linStops —
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
// Hazards are independently toggleable layers — each gets its own suffixed
// MapLibre source/layer pair (aots-mercator-wind vs aots-mercator-gust vs
// aots-mercator-river vs aots-mercator-rain, etc.) so any combination can be
// visible on the map at once (e.g. Wind + River together).
var _AOTS_HAZARDS = ['wind', 'gust', 'river', 'rain'];

// `suffix` (optional) — distinguishes EXTRA per-storm-group layers (see
// "MULTI-STORM GROUPS" section below) from the primary/default set, so a
// country hit by Storm B (not the primary-resolved Storm A) still gets its
// own real map tiles instead of silently reusing Storm A's. Omit/empty for
// the primary group — produces the exact same ids as before this existed.
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
// MERCATOR_TILE_PRECIP_MAT comments — keyed by COUNTRY + FORECAST_TIME(+
// RP_TIER / +THRESHOLD_MM+WINDOW_H), no STORM/TRACK_ID column exists for
// them) — but every tile-server endpoint still has a {storm} URL path
// segment for structural consistency with wind/gust, so river/rain requests
// fill it with an inert placeholder (config.storm, already a real non-empty
// string whenever a country/storm is resolved — reused rather than adding a
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
        return {
            storm: placeholderStorm, forecast_date: config.river_forecast_date,
            qs: '&hazard=river&rp_tier=' + encodeURIComponent(config.rp_tier),
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

// `group` (optional — see "MULTI-STORM GROUPS" below): a
// {country, storm, forecast_date, stats, admin_stats, suffix} bundle
// overriding the primary config's own country/storm/forecast_date/stats for
// this one extra layer pair, when a country is affected by a DIFFERENT real
// storm than the one the primary group already resolved (wind/gust only —
// river/rain aren't storm-scoped, see _hazardUrlParts's own comment, so a
// single shared forecast_date/rp_tier/threshold_mm already covers every
// selected country there... except when countries genuinely have different
// river/rain forecast_dates too, not yet handled by groups — same scope
// decision as the docstring in pages/map_shell_concept.py's
// _build_hazard_tile_config: wind/gust groups only for this round).
function applyHazardLayer(map, config, hazardKey, group) {
    var ids           = _hazardLayerIds(hazardKey, group && group.suffix);
    var parts         = group
        ? { storm: group.storm, forecast_date: group.forecast_date, qs: _hazardUrlParts(hazardKey, config).qs }
        : _hazardUrlParts(hazardKey, config);
    var country       = group ? group.country : config.country;
    // Real bug found+fixed here: for Wind/Gust specifically, the MapLibre
    // probability raster used to render regardless of tc-view-as ("Envelopes"
    // vs "Probability Raster" — pages/map_shell_concept.py's tc-view-as
    // SegmentedControl), even while "Envelopes" was selected — so both the
    // raster AND the Leaflet envelope polygons showed at once, when the
    // toggle's own labeling implies they're the two alternate, mutually
    // exclusive ways to view the SAME hazard. River/Rain have no envelope
    // concept at all (tc_view_as is a Tropical-Cyclone-only control) and
    // always render as raster regardless.
    var isTcHazard    = hazardKey === 'wind' || hazardKey === 'gust';
    var tcViewAs      = config.tc_view_as || 'envelopes';
    var visible       = !!config[hazardKey + '_visible'] && (!isTcHazard || tcViewAs === 'raster');
    var base          = config.tile_server_url != null ? config.tile_server_url : 'http://localhost:8001';
    // Vector tiles are fetched inside a MapLibre Web Worker which cannot resolve relative
    // URLs. Use window.location.origin as fallback when base is '' (SPCS proxy mode).
    var absBase       = base !== '' ? base : window.location.origin;
    var stats         = (group ? group.stats : config['stats_' + hazardKey]) || {};
    var adminStats    = (group ? group.admin_stats : config['admin_stats_' + hazardKey]) || stats;
    var tileProp      = config.tile_prop  || null;
    var adminProp     = config.admin_prop || null;
    var _defaultProp  = 'population';

    // River/rain requests have no real forecast_date to run without — skip
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

    var rasterUrl = base
        + '/tiles/raster/'
        + encodeURIComponent(country) + '/'
        + encodeURIComponent(parts.storm) + '/'
        + encodeURIComponent(parts.forecast_date)
        + '/' + (tileProp ? tileProp.toUpperCase() : 'POPULATION')
        + '/{z}/{x}/{y}.webp'
        + '?wind_threshold=' + (config.wind_threshold != null ? config.wind_threshold : 50)
        + parts.qs;

    if (map.getSource(ids.mercatorSource)) {
        map.getSource(ids.mercatorSource).setTiles([rasterUrl]);
    } else {
        map.addSource(ids.mercatorSource, {
            type: 'raster',
            tiles: [rasterUrl],
            tileSize: 256,
            minzoom: 3,
            maxzoom: 14,
        });
    }

    if (map.getSource(ids.adminSource)) {
        map.getSource(ids.adminSource).setTiles([adminUrl]);
    } else {
        map.addSource(ids.adminSource, {
            type: 'vector', tiles: [adminUrl], minzoom: 4, maxzoom: 10,
        });
    }

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
    // map_shell_concept.py's command bar) — Tiles (raster) and Regions
    // (admin polygons) are mutually exclusive, never shown at once.
    var viewMode = config.view_mode || 'tiles';
    if (tileProp && viewMode === 'tiles') setTileLayerProp(ids.tilesLayer, 'tiles', tileProp, stats, hazardKey, group);
    else setTileLayerVisibility(ids.tilesLayer, false);
    if (adminProp && visible && viewMode === 'admin') setTileLayerProp(ids.adminLayer, 'admin', adminProp, adminStats, hazardKey, group);
    else setTileLayerVisibility(ids.adminLayer, false);
}

// ---------------------------------------------------------------------------
// MULTI-STORM GROUPS: when selected countries are hit by genuinely
// DIFFERENT real storms on the same date (rare but real — confirmed via
// _build_hazard_tile_config's own multi-country resolution, which used to
// silently pick only the first-resolved storm for every selected country),
// each additional storm gets its own suffixed wind/gust source+layer pair
// so its own country's map tiles render for real instead of the primary
// group's storm being force-applied everywhere. config.extra_wind_groups /
// config.extra_gust_groups (arrays, possibly absent/empty — the overwhelming
// common case of one shared storm) drive this; the primary/default
// wind/gust layers above are completely unaffected when they're empty.
function _applyExtraHazardGroups(map, config) {
    ['wind', 'gust'].forEach(function (hazardKey) {
        var groups = config['extra_' + hazardKey + '_groups'] || [];
        var prevCount = (window._aots_extra_group_counts && window._aots_extra_group_counts[hazardKey]) || 0;
        groups.forEach(function (group, i) {
            group.suffix = 'x' + i;
            applyHazardLayer(map, config, hazardKey, group);
        });
        // Remove any surplus extra-group layers/sources left over from a
        // previous render with MORE groups than this one (e.g. a country
        // whose distinct storm was just deselected) — same-index suffixes
        // are reused across renders, so anything from `groups.length` up to
        // the old `prevCount` is now stale.
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

function applyTileConfig(config) {
    var map = window._aots_maplibre;
    console.log('[AoTS] applyTileConfig called, ready:', window._aots_maplibre_ready, 'config:', config && config.country);

    if (!map || !window._aots_maplibre_ready) {
        window._aots_pending_tile_config = config;
        console.log('[AoTS] MapLibre not ready — stored as pending config');
        return;
    }

    if (!config || !config.country) {
        _AOTS_HAZARDS.forEach(function (hazardKey) {
            var ids = _hazardLayerIds(hazardKey);
            [ids.tilesLayer, ids.adminLayer].forEach(function (id) {
                if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', 'none');
            });
        });
        return;
    }

    // Real bug found+fixed here: this assignment used to happen AFTER the
    // applyHazardLayer loop below. setTileLayerProp (called from inside
    // applyHazardLayer) doesn't receive `config` as a parameter — it reads
    // window._aots_tile_config directly to compute each layer's visibility
    // (`config[hazardKey + '_visible']`). With the assignment still pointing
    // at the PREVIOUS config at that moment, a hazard whose visibility flips
    // from off to on in this exact render (e.g. checking "Gust" for the
    // first time) got its layer set back to 'none' using the stale
    // pre-toggle value — confirmed live via Playwright: the Gust checkbox
    // stayed checked and the raster URL was correctly rebuilt, but the
    // layer's own visibility never flipped to 'visible', so nothing ever
    // rendered. Wind never showed this bug only because it's already
    // visible=true from the very first render. Moving this assignment BEFORE
    // the loop means every call this render already sees its own fresh
    // config, not last render's.
    window._aots_tile_config = config;

    _AOTS_HAZARDS.forEach(function (hazardKey) {
        applyHazardLayer(map, config, hazardKey);
    });
    _applyExtraHazardGroups(map, config);

    console.log('[AoTS] applyTileConfig done — layers in map:', map.getStyle().layers.map(function(l){return l.id;}));

    // Sync to current Leaflet viewport (Leaflet is the source of truth for position)
    var lMap = window._leaflet_maps && window._leaflet_maps['main-map'];
    if (lMap) {
        _syncMaplibreToLeaflet(lMap);
    }
}

window.applyTileConfig = applyTileConfig;

// Temporary, purely-visual "hide all hazards" preview (see the HAZARDS label
// click handler in pages/map_shell_concept.py) — never touches
// window._aots_tile_config itself, so un-hiding just re-applies it verbatim.
function setHazardsHiddenOverride(hidden) {
    var map = window._aots_maplibre;
    if (!map) return;
    if (hidden) {
        _AOTS_HAZARDS.forEach(function (hazardKey) {
            var ids = _hazardLayerIds(hazardKey);
            [ids.tilesLayer, ids.adminLayer].forEach(function (id) {
                if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', 'none');
            });
        });
    } else if (window._aots_tile_config) {
        applyTileConfig(window._aots_tile_config);
    }
}
window.setHazardsHiddenOverride = setHazardsHiddenOverride;

// ---------------------------------------------------------------------------
// 6. Layer toggle helpers
// ---------------------------------------------------------------------------

// `hazardKey`/`group` (optional, passed explicitly by applyHazardLayer) —
// falls back to deriving hazardKey from the layerId's own suffix (the
// original behavior, still correct for the PRIMARY/default group's plain
// ids like "aots-tiles-layer-wind") when omitted, for any other caller.
// Deliberately NOT derived this way when a `group` is given: an extra
// group's layer id (e.g. "aots-tiles-layer-wind-x0") has a numbered suffix
// as its last '-'-token, which isn't a real hazard key at all and would
// silently fall back to 'wind' even for a gust group — explicit params
// avoid that ambiguity entirely.
function setTileLayerProp(layerId, sourceLayer, prop, stats, hazardKey, group) {
    var map = window._aots_maplibre;
    if (!map) { console.warn('[AoTS] setTileLayerProp: no map'); return; }

    if (!hazardKey) {
        // Layer ids never contain another '-' inside the hazard key itself,
        // so the last '-'-separated token is always exactly the hazard key
        // — true only for the primary/default (non-grouped) id shape.
        var _idParts = layerId.split('-');
        hazardKey = _idParts[_idParts.length - 1];
        if (_AOTS_HAZARDS.indexOf(hazardKey) === -1) hazardKey = 'wind';
    }

    if (layerId.indexOf('aots-tiles-layer') === 0) {
        // Raster layer: change the tile URL to the new property
        var config = window._aots_tile_config || {};
        var base = config.tile_server_url != null ? config.tile_server_url : 'http://localhost:8001';
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

        var src = map.getSource(mercatorSource);
        if (src) src.setTiles([newUrl]);

        // Same tc-view-as gate as applyHazardLayer's own `visible` — Wind/
        // Gust's raster only shows in "Probability Raster" mode, not
        // "Envelopes" (see applyHazardLayer's own comment on this).
        var isTcHazard = hazardKey === 'wind' || hazardKey === 'gust';
        var tcViewAs = config.tc_view_as || 'envelopes';
        var hazardVisible = !!config[hazardKey + '_visible'] && (!isTcHazard || tcViewAs === 'raster');
        if (map.getLayer(layerId)) {
            map.setLayoutProperty(layerId, 'visibility', (prop && hazardVisible) ? 'visible' : 'none');
        }
        console.log('[AoTS] setTileLayerProp (raster)', layerId, prop, '→', colName);
        return;
    }

    // Admin layer: keep existing vector fill-color approach
    if (!map.getLayer(layerId)) {
        console.warn('[AoTS] setTileLayerProp: layer not found:', layerId, '— layers:', map.getStyle() ? map.getStyle().layers.map(function(l){return l.id;}) : 'no style');
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
// 8. Global raw layers — raw precip-rate raster + raw river-discharge raster
// ---------------------------------------------------------------------------
// UNLIKE every hazard in section 5 above (applyTileConfig/applyHazardLayer),
// these two layers are GLOBAL and country/storm-INDEPENDENT — a single
// worldwide tp/dis24 Zarr file covers the whole map for one forecast cycle
// (see services/tile_server.py's own "Global raw precipitation-rate
// endpoints"/"Global raw river-discharge endpoints" sections). They must
// keep rendering with no country selected at all (the default Global view),
// so this is deliberately a fully separate function/config/pending-config
// path from applyTileConfig — NOT folded into _AOTS_HAZARDS or gated by
// config.country the way every other hazard layer is.
//
// Visibility is driven by the EXISTING ms-river-on ("River Flooding")/
// ms-rain-on ("Rainfall") checkboxes (config.precip_visible/river_visible,
// resolved by _build_global_raw_config in pages/map_shell_concept.py) — no
// dedicated raw-layer checkboxes anymore. config.rain_mode ("mean"|
// "probability", from the flood-view-as SegmentedControl, now nested under
// Rainfall's own controls) selects which server-side aggregation the
// precip-raw endpoint renders — RAIN ONLY. River has no Mean mode (removed
// per explicit user request: unlike rain, it has no second independent
// quantity, so a Mean toggle there was always describing the identical
// per-cell member-agreement fraction under a different name/colour) and
// always renders Probability, with no mode query param at all. Both
// endpoints are fully pre-colored server-side (fixed breakpoint ramps), so
// there is no client-side color/radius styling needed for either layer.
//
// River-raw used to be a sparse vector/circle layer (one point per discharge
// cell) — the user explicitly flagged that as wrong ("I only see points not
// proper rasters this doesn't look right"). It's now a real interpolated
// raster from the same /tiles/raster/{precip-raw,river-raw}/... family as
// precip-raw, just with source id 'river' vs 'precip'.

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
    var rainMode = (config.rain_mode === 'probability') ? 'probability' : 'mean';

    // --- Precip-raw raster (radar-style rain-rate / exceedance-probability tiles) ---
    // window_h/threshold_mm (resolved by _build_global_raw_config in
    // pages/map_shell_concept.py from the SAME ms-rain-window/ms-rain-slider
    // controls the country-scoped rain hazard already uses — see
    // _hazardUrlParts's own 'rain' branch above for the identical query-param
    // naming convention) — real bug fixed here: these two controls used to
    // do nothing for this raw layer, which always rendered a hardcoded
    // server-side default of 6h/10mm regardless of what was selected.
    var precipTime = config.precip_forecast_time;
    if (precipTime) {
        var precipWindowH = config.window_h != null ? config.window_h : 6;
        var precipThresholdMm = config.threshold_mm != null ? config.threshold_mm : 10.0;
        var precipUrl = base + '/tiles/raster/precip-raw/' + encodeURIComponent(precipTime)
            + '/{z}/{x}/{y}.webp?mode=' + rainMode
            + '&window_h=' + precipWindowH + '&threshold_mm=' + precipThresholdMm;
        var precipSrc = map.getSource(ids.precipSource);
        if (precipSrc) {
            precipSrc.setTiles([precipUrl]);
        } else {
            map.addSource(ids.precipSource, {
                type: 'raster', tiles: [precipUrl], tileSize: 256, minzoom: 0, maxzoom: 14,
            });
        }
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
        // No real tp data at all (genuinely empty environment) — hide rather
        // than point at a forecast_time that doesn't exist.
        map.setLayoutProperty(ids.precipLayer, 'visibility', 'none');
    }

    // --- River-raw raster (interpolated discharge / exceedance-probability tiles) ---
    // rp_tier (resolved by _build_global_raw_config from ms-river-slider) —
    // real bug fixed here: this used to always request the server's default
    // rp10 regardless of what the slider was set to. No mode param — the endpoint
    // always renders Probability.
    var riverTime = config.river_forecast_time;
    var riverRpTier = config.rp_tier || 'rp10';
    if (riverTime) {
        var riverUrl = base + '/tiles/raster/river-raw/' + encodeURIComponent(riverTime)
            + '/{z}/{x}/{y}.webp?rp_tier=' + riverRpTier;
        var riverSrc = map.getSource(ids.riverSource);
        if (riverSrc) {
            riverSrc.setTiles([riverUrl]);
        } else {
            map.addSource(ids.riverSource, {
                type: 'raster', tiles: [riverUrl], tileSize: 256, minzoom: 0, maxzoom: 14,
            });
        }
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
        // No real dis24 data at all — hide rather than point at a
        // forecast_time that doesn't exist.
        map.setLayoutProperty(ids.riverLayer, 'visibility', 'none');
    }
}

window.applyGlobalRawConfig = applyGlobalRawConfig;

// ---------------------------------------------------------------------------
// Global loading indicator (top bar, next to the country selector — see
// pages/map_shell_concept.py's _ms_loading_badge). Real bug found+fixed
// here: dcc.Loading's own target_components mechanism never actually shows
// a spinner for ms-tile-config-store, even nested as a direct child of the
// Loading component (confirmed live via Playwright — polled the DOM every
// 150ms through multiple genuinely-slow, 1.6-3.0s measured real callback
// round-trips and it never appeared). Dash's OWN generic top-level
// indicator (a `._dash-loading-callback`-classed div Dash itself inserts
// directly under #react-entry-point while ANY callback is in flight) DID
// reliably appear for the exact same requests in the same test — so this
// reuses that already-proven signal via a plain MutationObserver instead of
// trusting target_components again.
// Real gap found+fixed here: the Dash-callback signal above never covers
// MapLibre's OWN tile network activity (raster WebP hazard tiles, MVT
// vector tiles) — those load via MapLibre GL's internal networking, kicked
// off directly from applyHazardLayer/setTileLayerProp with no Dash
// callback (server OR clientside) involved at all. That's exactly what a
// user watching colours paint onto the map calls "the layers loading", so
// the badge needs to reflect it too, not just Dash's own request/response
// cycle. window._aots_map_tiles_loading is toggled by MapLibre's own
// 'dataloading'/'idle' events (wired up right after the map is constructed
// in initMaplibre above) and OR'd into the same check.
window._aots_map_tiles_loading = false;
function _initGlobalLoadingIndicator() {
    var badge = document.getElementById('ms-global-loading-indicator');
    var root = document.getElementById('react-entry-point');
    if (!badge || !root) {
        // Layout not mounted yet on first DOMContentLoaded fire — retry
        // shortly rather than silently giving up (same "poll until ready"
        // pattern initMaplibre already uses above for the map container).
        setTimeout(_initGlobalLoadingIndicator, 200);
        return;
    }
    var check = function () {
        var isLoading = !!root.querySelector('._dash-loading-callback')
            || !!window._aots_map_tiles_loading
            || (window._aots_pending_fetches || 0) > 0;
        badge.style.display = isLoading ? '' : 'none';
    };
    // Exposed globally so the MapLibre 'dataloading'/'idle' handlers (which
    // don't mutate the Dash-rendered DOM the MutationObserver below
    // watches) can force an immediate re-check instead of waiting for an
    // unrelated DOM mutation to happen to fire it.
    window._aots_checkLoadingIndicator = check;
    new MutationObserver(check).observe(root, { attributes: true, childList: true, subtree: true });
    check();
}
document.addEventListener('DOMContentLoaded', _initGlobalLoadingIndicator);
// Also try immediately (same reasoning as initMaplibre above — dash-render
// may have already fired DOMContentLoaded by the time this script runs).
_initGlobalLoadingIndicator();

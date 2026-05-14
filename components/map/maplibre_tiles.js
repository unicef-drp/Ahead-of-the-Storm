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
    if (!container || window._aots_maplibre) return;

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

    // Register pmtiles:// protocol so MapLibre can read PMTiles files directly
    // from Snowflake stage pre-signed URLs via HTTP range requests.
    if (window.pmtiles && !window._aots_pmtiles_registered) {
        var protocol = new window.pmtiles.Protocol();
        maplibregl.addProtocol('pmtiles', protocol.tile.bind(protocol));
        window._aots_pmtiles_registered = true;
    }

    window._aots_maplibre = new maplibregl.Map({
        container: 'maplibre-container',
        style: style,
        center: [0, 20],
        zoom: 2,
        interactive: false,        // Leaflet handles all mouse/touch events
        attributionControl: false,
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
    });
}

// Try immediately on DOMContentLoaded, then poll
document.addEventListener('DOMContentLoaded', initMaplibre);
var _initInterval = setInterval(function () {
    if (document.getElementById('maplibre-container') && !window._aots_maplibre) initMaplibre();
    if (window._aots_maplibre) clearInterval(_initInterval);
}, 500);

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
    var isAdmin = feature.layer && feature.layer.id === 'aots-admin-layer';

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
              + '<div style="font-size:11px;color:#555;">Hurricane Impact Probability: ' + _fmtPct(prob) + '</div>'
              + '<hr style="margin:5px 0;border:none;border-top:1px solid #ddd;">';
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
            var adminFeatures = map.queryRenderedFeatures([pt.x, pt.y], { layers: ['aots-admin-layer'] });
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
        var adminFeatures = map.queryRenderedFeatures([pt.x, pt.y], { layers: ['aots-admin-layer'] });
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

        // For the raster tile layer, use the API endpoint
        var tilesLayerVisible = map.getLayer('aots-tiles-layer') &&
            map.getLayoutProperty('aots-tiles-layer', 'visibility') === 'visible';
        if (!tilesLayerVisible) {
            el.style.display = 'none';
            return;
        }

        // Cancel previous pending request
        if (_pending_request) { _pending_request._cancelled = true; }

        var req = { _cancelled: false };
        _pending_request = req;

        var url = (config.tile_server_url || 'http://localhost:8001')
            + '/tile-value/'
            + encodeURIComponent(config.country) + '/'
            + encodeURIComponent(config.storm) + '/'
            + encodeURIComponent(config.forecast_date)
            + '?lon=' + lon.toFixed(6)
            + '&lat=' + lat.toFixed(6)
            + '&wind_threshold=' + config.wind_threshold;

        fetch(url)
            .then(function(r) { return r.json(); })
            .then(function(props) {
                if (req._cancelled) return;
                if (!props || Object.keys(props).length === 0) {
                    el.style.display = 'none';
                    return;
                }
                // Build a fake feature object compatible with _buildTileTooltip
                var feature = { properties: props, layer: { id: 'aots-tiles-layer' } };
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

function applyTileConfig(config) {
    var map = window._aots_maplibre;
    console.log('[AoTS] applyTileConfig called, ready:', window._aots_maplibre_ready, 'config:', config && config.country);

    if (!map || !window._aots_maplibre_ready) {
        window._aots_pending_tile_config = config;
        console.log('[AoTS] MapLibre not ready — stored as pending config');
        return;
    }

    var layerIds = ['aots-tiles-layer', 'aots-admin-layer'];

    if (!config || !config.country) {
        layerIds.forEach(function (id) {
            if (map.getLayer(id)) map.setLayoutProperty(id, 'visibility', 'none');
        });
        return;
    }

    var country       = config.country;
    var storm         = config.storm;
    var forecast_date = config.forecast_date;
    var wind_threshold = config.wind_threshold;
    var base          = config.tile_server_url || 'http://localhost:8001';
    var stats         = config.stats || {};
    var adminStats    = config.admin_stats || stats;
    var tileProp      = config.tile_prop  || null;
    var adminProp     = config.admin_prop || null;

    var adminUrl = base
        + '/tiles/admin/'
        + encodeURIComponent(country) + '/'
        + encodeURIComponent(storm) + '/'
        + encodeURIComponent(forecast_date)
        + '/{z}/{x}/{y}.pbf'
        + '?wind_threshold=' + wind_threshold
        + '&admin_level=1';

    // --- Mercator (H3) raster tile source ---
    var rasterUrl = base
        + '/tiles/raster/'
        + encodeURIComponent(country) + '/'
        + encodeURIComponent(storm) + '/'
        + encodeURIComponent(forecast_date)
        + '/' + (tileProp ? tileProp.toUpperCase() : 'POPULATION')
        + '/{z}/{x}/{y}.webp'
        + '?wind_threshold=' + wind_threshold;

    if (map.getSource('aots-mercator')) {
        map.getSource('aots-mercator').setTiles([rasterUrl]);
    } else {
        map.addSource('aots-mercator', {
            type: 'raster',
            tiles: [rasterUrl],
            tileSize: 512,
            minzoom: 5,
            maxzoom: 10,
        });
    }

    // --- Admin tile source ---
    if (map.getSource('aots-admin')) {
        map.getSource('aots-admin').setTiles([adminUrl]);
    } else {
        map.addSource('aots-admin', {
            type: 'vector', tiles: [adminUrl], minzoom: 4, maxzoom: 10,
        });
    }

    var _defaultProp = 'population';

    // --- Mercator raster layer ---
    if (!map.getLayer('aots-tiles-layer')) {
        map.addLayer({
            id: 'aots-tiles-layer',
            type: 'raster',
            source: 'aots-mercator',
            layout: { visibility: tileProp ? 'visible' : 'none' },
            paint: {
                'raster-opacity': 0.8,
                'raster-fade-duration': 0,
                'raster-resampling': 'nearest',
            }
        });
    }

    // --- Admin fill layer ---
    if (!map.getLayer('aots-admin-layer')) {
        map.addLayer({
            id: 'aots-admin-layer',
            type: 'fill',
            source: 'aots-admin',
            'source-layer': 'admin',
            layout: { visibility: 'none' },
            paint: {
                'fill-color': buildColorExpression(adminProp || _defaultProp, adminStats),
                'fill-opacity': 0.6,
                'fill-outline-color': 'rgba(0,0,0,0.2)',
            }
        });
    } else if (adminProp) {
        map.setPaintProperty('aots-admin-layer', 'fill-color', buildColorExpression(adminProp, adminStats));
    }

    console.log('[AoTS] applyTileConfig done — layers in map:', map.getStyle().layers.map(function(l){return l.id;}));

    // Only show layers if a prop was explicitly selected (not null).
    // When tile_prop is null (no layer selected), the layer stays hidden until
    // the user selects a layer via the radio buttons → visibility callback.
    if (tileProp) setTileLayerProp('aots-tiles-layer', 'tiles', tileProp, stats);
    if (adminProp) setTileLayerProp('aots-admin-layer', 'admin', adminProp, adminStats);

    // Sync to current Leaflet viewport (Leaflet is the source of truth for position)
    var lMap = window._leaflet_maps && window._leaflet_maps['main-map'];
    if (lMap) {
        _syncMaplibreToLeaflet(lMap);
    }

    window._aots_tile_config = config;
}

window.applyTileConfig = applyTileConfig;

/**
 * Switch the base (non-impact) tile sources to PMTiles for instant loading.
 * Called from Dash after pre-signed URLs are fetched from /pmtiles-url/.
 * Falls back gracefully if URLs are null/unavailable.
 */
function switchBaseLayerToPMTiles(country, tilesUrl, adminUrl) {
    var map = window._aots_maplibre;
    if (!map || !window._aots_maplibre_ready) return;

    if (tilesUrl) {
        var existingSrc = map.getSource('aots-mercator');
        if (existingSrc) {
            // Remove and replace the existing raster tile server source with PMTiles
            if (map.getLayer('aots-tiles-layer')) map.removeLayer('aots-tiles-layer');
            map.removeSource('aots-mercator');
        }
        // PMTiles for the mercator layer is still served as vector PMTiles,
        // but re-use the raster tile server URL approach when PMTiles unavailable.
        // For now, re-add as raster source pointing to the tile server URL so
        // the layer type remains consistent (raster).
        var config = window._aots_tile_config || {};
        var tileProp = (config.tile_prop) || null;
        var base = config.tile_server_url || 'http://localhost:8001';
        var storm = config.storm;
        var forecast_date = config.forecast_date;
        var wind_threshold = config.wind_threshold;
        var rasterUrl = base
            + '/tiles/raster/'
            + encodeURIComponent(country) + '/'
            + encodeURIComponent(storm) + '/'
            + encodeURIComponent(forecast_date)
            + '/' + (tileProp ? tileProp.toUpperCase() : 'POPULATION')
            + '/{z}/{x}/{y}.webp'
            + '?wind_threshold=' + wind_threshold;
        map.addSource('aots-mercator', {
            type: 'raster',
            tiles: [rasterUrl],
            tileSize: 512,
            minzoom: 5,
            maxzoom: 10,
        });
        map.addLayer({
            id: 'aots-tiles-layer',
            type: 'raster',
            source: 'aots-mercator',
            layout: { visibility: tileProp ? 'visible' : 'none' },
            paint: {
                'raster-opacity': 0.8,
                'raster-fade-duration': 0,
                'raster-resampling': 'nearest',
            }
        });
        console.log('[AoTS] Switched aots-mercator to raster (PMTiles path not used for raster source)');
    }

    if (adminUrl) {
        var existingAdmin = map.getSource('aots-admin');
        if (existingAdmin) {
            map.removeLayer('aots-admin-layer');
            map.removeSource('aots-admin');
        }
        map.addSource('aots-admin', {
            type: 'vector',
            url: 'pmtiles://' + adminUrl,
            minzoom: 2,
            maxzoom: 10,
        });
        var config = window._aots_tile_config || {};
        var stats = (config.stats) || {};
        var adminStats = (config.admin_stats) || stats;
        var adminProp = (config.admin_prop) || null;
        map.addLayer({
            id: 'aots-admin-layer',
            type: 'fill',
            source: 'aots-admin',
            'source-layer': 'admin',
            layout: { visibility: adminProp ? 'visible' : 'none' },
            paint: {
                'fill-color': buildColorExpression(adminProp || 'population', adminStats),
                'fill-opacity': 0.6,
                'fill-outline-color': 'rgba(0,0,0,0.2)',
            }
        });
        console.log('[AoTS] Switched aots-admin to PMTiles');
    }
}

window.switchBaseLayerToPMTiles = switchBaseLayerToPMTiles;

// ---------------------------------------------------------------------------
// 6. Layer toggle helpers
// ---------------------------------------------------------------------------

function setTileLayerProp(layerId, sourceLayer, prop, stats) {
    var map = window._aots_maplibre;
    if (!map) { console.warn('[AoTS] setTileLayerProp: no map'); return; }

    if (layerId === 'aots-tiles-layer') {
        // Raster layer: change the tile URL to the new property
        var config = window._aots_tile_config || {};
        var base = config.tile_server_url || 'http://localhost:8001';
        var country = config.country;
        var storm = config.storm;
        var forecast_date = config.forecast_date;
        var wind_threshold = config.wind_threshold;

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
            + encodeURIComponent(storm) + '/'
            + encodeURIComponent(forecast_date)
            + '/' + colName
            + '/{z}/{x}/{y}.webp'
            + '?wind_threshold=' + wind_threshold;

        var src = map.getSource('aots-mercator');
        if (src) src.setTiles([newUrl]);

        if (map.getLayer(layerId)) {
            map.setLayoutProperty(layerId, 'visibility', prop ? 'visible' : 'none');
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
    }
};

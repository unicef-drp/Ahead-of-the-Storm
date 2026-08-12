"""
Dash Leaflet JavaScript Styling Functions
Contains JavaScript functions for styling map layers and tooltips
"""
from dash_extensions.javascript import assign

# =============================================================================
# MAPLIBRE SYNC: attached to dl.Map eventHandlers
# =============================================================================
# These fire on every Leaflet map event and keep MapLibre in sync.
# Using Leaflet's own event system is more reliable than patching L.Map.prototype.

sync_maplibre_on_load = assign("""
function(e) {
    var lMap = e.target;
    window._leaflet_maps = window._leaflet_maps || {};
    window._leaflet_maps['main-map'] = lMap;
    window._aots_leaflet_ready_map = lMap;
    if (window._aots_maplibre && window._aots_maplibre_ready) {
        var c = lMap.getCenter();
        window._aots_maplibre.jumpTo({ center: [c.lng, c.lat], zoom: lMap.getZoom() - 1 });
    }
}
""")

sync_maplibre_on_move = assign("""
function(e) {
    var lMap = e.target;
    window._leaflet_maps = window._leaflet_maps || {};
    window._leaflet_maps['main-map'] = lMap;
    if (window._aots_maplibre && window._aots_maplibre_ready) {
        var c = lMap.getCenter();
        window._aots_maplibre.jumpTo({ center: [c.lng, c.lat], zoom: lMap.getZoom() - 1 });
    }
}
""")

# =============================================================================
# LAYER STYLING FUNCTIONS
# =============================================================================

# JavaScript styling for hurricane tracks
style_tracks = assign("""
function(feature, context) {
    const member_type = feature.properties?.member_type;
    if (member_type === 'control') {
        return {color: '#ff0000', weight: 4, opacity: 1.0};
    } else {
        return {color: '#1cabe2', weight: 2, opacity: 0.8};
    }
}
""")

# NOTE: style_tiles, tooltip_tiles, tooltip_admin are intentionally absent.
# Tile and admin layers are rendered entirely by MapLibre GL (maplibre_tiles.js).
# The dl.GeoJSON layers for tiles/admin always receive empty data so these callbacks
# would never execute. Styling and tooltips live in components/map/maplibre_tiles.js.

# JavaScript point-to-layer function for schools and health centers
point_to_layer_schools_health = assign("""
function(feature, latlng, context) {
    const props = feature.properties || {};
    const color = props._color || '#808080';
    const radius = props._radius || 12;
    const opacity = props._opacity || 0.8;
    const weight = props._weight || 2;
    const fillOpacity = props._fillOpacity || 0.7;

    return L.circleMarker(latlng, {
        radius: radius,
        fillColor: color,
        color: color,
        weight: weight,
        opacity: opacity,
        fillOpacity: fillOpacity
    });
}
""")

style_envelopes = assign("""
function(feature, context) {
    const props = feature.properties || {};
    // Distinguishes two genuinely different cases that a naive
    // `props.severity_population || 0` would collapse into the same falsy 0:
    // (a) no real severity_population attributable at all (Global mode, or
    // no country selected: the key is entirely ABSENT from properties, see
    // _build_ms_envelope_geojson's own comment in pages/map_shell_concept.py)
    // vs (b) a real, CONFIRMED zero for this specific member (Country
    // Analysis mode, the key IS present with value 0. TRACK_MAT has one row
    // per ensemble member, so a member with no real exposure genuinely has
    // SEVERITY_POPULATION=0, not a missing row). These get different
    // colors: (a) gets the orange/yellow "consensus" overlap-density
    // fill below; (b) gets a distinct grey instead, so a member
    // confirmed to have NO impact never looks like it might have some.
    const hasSeverityData = props.severity_population !== undefined && props.severity_population !== null;
    const severity_population = hasSeverityData ? props.severity_population : 0;
    const max_population = props.max_population || 1;
    const isGust = props.hazard === 'gust';

    // "Consensus" rendering (case (a) above): no real per-member
    // severity_population to color by at all. A low, uniform,
    // near-borderless fill so the real per-member envelope polygons (up to
    // 51 of them, all real geometry) alpha-blend into a natural density
    // gradient where they overlap: darker = more members agree this area
    // is threatened, lighter = fewer. This is a genuine union-with-overlap-
    // count effect achieved via the browser's own alpha compositing, not a
    // separate computed grid. Gust uses its own lighter orange (#ffa94d,
    // matching its GUST color constant in pages/map_shell_concept.py) so
    // Wind and Gust envelopes stay visually distinguishable when both are
    // shown at once (both hazards share one combined FeatureCollection).
    if (!hasSeverityData) {
        const c = isGust ? '#ffa94d' : '#e8590c';
        return {color: c, weight: 0.5, opacity: 0.25, fillColor: c, fillOpacity: 0.09};
    }

    // Case (b) above: real severity data exists for this member and it's
    // confirmed exactly zero: a plain, muted grey, NOT part of the
    // yellow->red severity gradient below (that gradient is reserved for
    // members with SOME real impact, how much of it), and NOT the
    // orange/yellow consensus fill either (that would misleadingly suggest
    // this member is just an "unknown" case like Global mode, when it's
    // actually a confirmed, real zero).
    if (severity_population === 0) {
        return {color: '#adb5bd', weight: 0.5, opacity: 0.4, fillColor: '#adb5bd', fillOpacity: 0.12};
    }

    // Calculate relative severity (0 to 1)
    const relativeSeverity = Math.min(severity_population / max_population, 1);

    // Smooth gradient from yellow to red using color interpolation
    // Using cubic easing for smoother transitions
    const easedSeverity = relativeSeverity * relativeSeverity * relativeSeverity;

    // Color interpolation helper
    const interpolateColor = (startColor, endColor, fraction) => {
        const start = parseInt(startColor.slice(1), 16);
        const end = parseInt(endColor.slice(1), 16);
        const r = Math.round(((start >> 16) & 0xff) * (1 - fraction) + ((end >> 16) & 0xff) * fraction);
        const g = Math.round(((start >> 8) & 0xff) * (1 - fraction) + ((end >> 8) & 0xff) * fraction);
        const b = Math.round((start & 0xff) * (1 - fraction) + (end & 0xff) * fraction);
        return '#' + ((1 << 24) + (r << 16) + (g << 8) + b).toString(16).slice(1);
    };

    // Wind: yellow (#FFFF00) -> dark red (#8B0000). Gust: pale yellow
    // (#FFF3BF) -> burnt orange (#D9480F), a visually distinct gradient
    // family so a user can tell which hazard a colored envelope belongs to
    // at a glance, not just via the tooltip.
    const color = isGust
        ? interpolateColor('#FFF3BF', '#D9480F', easedSeverity)
        : interpolateColor('#FFFF00', '#8B0000', easedSeverity);

    // Opacity increases with severity: 0.3 to 0.9 range
    const fillOpacity = 0.3 + (easedSeverity * 0.6);
    return {color: color, weight: 2, fillColor: color, fillOpacity: fillOpacity};
}
""")

# =============================================================================
# TOOLTIP FUNCTIONS
# =============================================================================
# Functions for displaying data on hover

tooltip_tracks = assign("""
function(feature, layer) {
    const props = feature.properties || {};
    const escapeHtml = (s) => {
        if (typeof s !== 'string') return s;
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#x27;');
    };
    // _mapT/_AOTS_TT_* are global window functions/vars set by
    // assets/maplibre_tiles.js (same browser window, loaded as a plain
    // non-module <script>), providing translated tooltip strings and
    // theme-aware colors/font sizes. See _mapT's own comment for the full
    // rationale and window.AOTS_MAP_I18N's source (pages/map_shell_
    // concept.py's _MAP_TOOLTIP_TRANSLATIONS via ms-map-i18n-store).
    const member_raw = props.ensemble_member;
    const member = member_raw != null ? escapeHtml(String(member_raw)) : null;
    const type = props.member_type || 'N/A';
    const storm = props.track_id ? escapeHtml(String(props.track_id)) : null;

    const label = type === 'control' ? _mapT('Control Track') : _mapT('Ensemble Track');

    const content = `
        <div style="font-size: 13px; font-weight: 600; color: #1cabe2; margin-bottom: 5px;">
            ${storm ? storm + ' — ' : ''}${label}
        </div>
        <div style="font-size: 11px; color: ${_AOTS_TT_VALUE};">
            <strong>${_mapT('Ensemble Member')}:</strong> ${member !== null ? '#' + member : _mapT('N/A')}
        </div>
    `;

    layer.bindTooltip(content, {sticky: true});
}
""")

tooltip_envelopes = assign("""
function(feature, layer) {
    const props = feature.properties || {};
    const escapeHtml = (s) => {
        if (typeof s !== 'string') return s;
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#x27;');
    };
    const isGust = props.hazard === 'gust';
    const wind_threshold = escapeHtml(String(props.wind_threshold ?? props.WIND_THRESHOLD ?? 'N/A'));
    const ensemble_member_raw = props.ensemble_member ?? props.ENSEMBLE_MEMBER;
    const ensemble_member = ensemble_member_raw != null ? escapeHtml(String(ensemble_member_raw)) : 'N/A';
    const severity_population = props.severity_population;
    const severity_school_age_population = props.severity_school_age_population;
    const severity_infant_population = props.severity_infant_population;
    const severity_adolescent_population = props.severity_adolescent_population;
    const severity_schools = props.severity_schools;
    const severity_hcs = props.severity_hcs;
    const severity_num_shelters = props.severity_num_shelters;
    const severity_num_wash = props.severity_num_wash;
    const severity_built_surface_m2 = props.severity_built_surface_m2;
    const severity_people_in_need = props.severity_people_in_need;
    const severity_children_in_need = props.severity_children_in_need;
    const severity_infant_in_need = props.severity_infant_in_need;
    const severity_school_age_in_need = props.severity_school_age_in_need;
    const severity_adolescent_in_need = props.severity_adolescent_in_need;

    const formatNumber = (num) => {
        if (typeof num === 'number') {
            return new Intl.NumberFormat('en-US').format(Math.ceil(num));
        }
        return num;
    };

    // null/NaN → 'N/A'; 0 → '0' (confirmed no impact); >0 → formatted number
    const fmtImpact = (val) => {
        if (val == null || (typeof val === 'number' && isNaN(val))) return 'N/A';
        return val > 0 ? formatNumber(val) : '0';
    };

    // null/NaN → 'N/A'; 0 → 'N/A' (0 m² is meaningless); >0 → 'X m²'
    const fmtSurface = (val) => {
        if (val == null || (typeof val === 'number' && isNaN(val)) || val <= 0) return 'N/A';
        return formatNumber(val) + ' m²';
    };

    // Always show same structure, use N/A when data not available
    let content = `
        <div style="font-size: 13px; font-weight: 600; color: #ff0000; margin-bottom: 5px;">
            ${isGust ? _mapT('Gust Envelope') : _mapT('Hurricane Envelope')}
        </div>
        <div style="font-size: 11px; color: ${_AOTS_TT_VALUE};">
            <strong>${isGust ? _mapT('Gust Threshold') : _mapT('Wind Threshold')}:</strong> ${wind_threshold}
        </div>
        <div style="font-size: 11px; color: ${_AOTS_TT_VALUE};">
            <strong>${_mapT('Ensemble Member')}:</strong> ${ensemble_member !== 'N/A' ? '#' + ensemble_member : _mapT('N/A')}
        </div>
    `;

    // Children total: N/A only when all components are null; 0 when all are confirmed 0
    const _isNoData = v => v == null || (typeof v === 'number' && isNaN(v));
    const _sev_children_all_null = _isNoData(severity_infant_population) && _isNoData(severity_school_age_population) && _isNoData(severity_adolescent_population);
    const sev_children_total = _sev_children_all_null ? null : (severity_infant_population || 0) + (severity_school_age_population || 0) + (severity_adolescent_population || 0);

    content += `
        <hr style="margin: 5px 0; border: none; border-top: 1px solid ${_AOTS_TT_DIVIDER};">
        <div style="font-size: 11px; color: ${_AOTS_TT_LABEL}; margin-top: 5px;">
            <strong>${_mapT('Impact')}:</strong>
        </div>
        <div style="font-size: 11px; color: ${_AOTS_TT_VALUE};">
            ${_mapT('Population')}: ${fmtImpact(severity_population)}
        </div>
        <div style="font-size: 11px; color: ${_AOTS_TT_VALUE};">
            ${_mapT('Children (total)')}: ${sev_children_total !== null ? fmtImpact(sev_children_total) : _mapT('N/A')}
        </div>
        <div style="font-size: 10px; color: ${_AOTS_TT_SUB}; padding-left: 10px; font-style: italic;">
            ${_mapT('Age 0–4')}: ${fmtImpact(severity_infant_population)}
        </div>
        <div style="font-size: 10px; color: ${_AOTS_TT_SUB}; padding-left: 10px; font-style: italic;">
            ${_mapT('Age 5–14')}: ${fmtImpact(severity_school_age_population)}
        </div>
        <div style="font-size: 10px; color: ${_AOTS_TT_SUB}; padding-left: 10px; font-style: italic;">
            ${_mapT('Age 15–19')}: ${fmtImpact(severity_adolescent_population)}
        </div>
        <div style="font-size: 11px; color: ${_AOTS_TT_VALUE};">
            ${_mapT('Schools')}: ${fmtImpact(severity_schools)}
        </div>
        <div style="font-size: 11px; color: ${_AOTS_TT_VALUE};">
            ${_mapT('Health Centers')}: ${fmtImpact(severity_hcs)}
        </div>
        <div style="font-size: 11px; color: ${_AOTS_TT_VALUE};">
            ${_mapT('Shelters')}: ${fmtImpact(severity_num_shelters)}
        </div>
        <div style="font-size: 11px; color: ${_AOTS_TT_VALUE};">
            ${_mapT('WASH Facilities')}: ${fmtImpact(severity_num_wash)}
        </div>
        <div style="font-size: 11px; color: ${_AOTS_TT_VALUE};">
            ${_mapT('Built Surface')}: ${fmtSurface(severity_built_surface_m2)}
        </div>
    `;

    const _hasInNeed = v => v != null && typeof v === 'number' && !isNaN(v) && v > 0;
    if (_hasInNeed(severity_people_in_need) || _hasInNeed(severity_children_in_need)) {
        const sev_chin_all_null = severity_infant_in_need == null && severity_school_age_in_need == null && severity_adolescent_in_need == null;
        const sev_chin_total = sev_chin_all_null ? null : (severity_infant_in_need || 0) + (severity_school_age_in_need || 0) + (severity_adolescent_in_need || 0);
        content += `
        <hr style="margin: 5px 0; border: none; border-top: 1px solid ${_AOTS_TT_DIVIDER};">
        <div style="font-size: 11px; color: #f59f00; font-weight: 600; margin-top: 5px;">${_mapT('In Need')}:</div>
        <div style="font-size: 11px; color: #f59f00;">${_mapT('Population')}: ${_hasInNeed(severity_people_in_need) ? formatNumber(severity_people_in_need) : _mapT('N/A')}</div>
        <div style="font-size: 11px; color: #f59f00;">${_mapT('Children (total)')}: ${sev_chin_total !== null ? formatNumber(sev_chin_total) : (_hasInNeed(severity_children_in_need) ? formatNumber(severity_children_in_need) : _mapT('N/A'))}</div>
        <div style="font-size: 10px; color: #f59f00; padding-left: 10px; font-style: italic;">${_mapT('Age 0–4')}: ${_hasInNeed(severity_infant_in_need) ? formatNumber(severity_infant_in_need) : _mapT('N/A')}</div>
        <div style="font-size: 10px; color: #f59f00; padding-left: 10px; font-style: italic;">${_mapT('Age 5–14')}: ${_hasInNeed(severity_school_age_in_need) ? formatNumber(severity_school_age_in_need) : _mapT('N/A')}</div>
        <div style="font-size: 10px; color: #f59f00; padding-left: 10px; font-style: italic;">${_mapT('Age 15–19')}: ${_hasInNeed(severity_adolescent_in_need) ? formatNumber(severity_adolescent_in_need) : _mapT('N/A')}</div>
        `;
    }

    layer.bindTooltip(content, {sticky: true});
}
""")

tooltip_schools = assign("""
function(feature, layer) {
    const props = feature.properties || {};
    const escapeHtml = (s) => {
        if (typeof s !== 'string') return s;
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#x27;');
    };
    const probability = props.probability;
    const school_name = escapeHtml(props.school_name || props.name || props.school || null);

    const formatPercent = (prob) => {
        if (typeof prob === 'number') {
            return (prob * 100).toFixed(1) + '%';
        }
        return 'N/A';
    };

    let content = `
        <div style="font-size: 13px; font-weight: 600; color: #4169E1; margin-bottom: 5px;">
            ${_mapT('School')}
        </div>
        ${school_name ? `<div style="font-size: 11px; color: ${_AOTS_TT_VALUE};"><strong>${_mapT('Name')}:</strong> ${school_name}</div>` : ''}
    `;
    if (probability !== undefined && probability !== null) {
        content += `<div style="font-size: 11px; color: ${_AOTS_TT_VALUE};"><strong>${_mapT('Impact Probability')}:</strong> ${formatPercent(probability)}</div>`;
    } else {
        content += `<div style="font-size: 11px; color: ${_AOTS_TT_SUB}; font-style: italic;">${_mapT('Base location (no impact data)')}</div>`;
    }

    layer.bindTooltip(content, {sticky: true});
}
""")

tooltip_health = assign("""
function(feature, layer) {
    const props = feature.properties || {};
    const escapeHtml = (s) => {
        if (typeof s !== 'string') return s;
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#x27;');
    };
    const probability = props.probability;
    const facility_name = escapeHtml(props.facility_name || props.name || props.amenity_name || null);
    const facility_type = escapeHtml(props.facility_type || props.amenity_type || props.type || null);

    const formatPercent = (prob) => {
        if (typeof prob === 'number') {
            return (prob * 100).toFixed(1) + '%';
        }
        return 'N/A';
    };

    let content = `
        <div style="font-size: 13px; font-weight: 600; color: #228B22; margin-bottom: 5px;">
            ${_mapT('Health Facility')}
        </div>
        ${facility_name ? `<div style="font-size: 11px; color: ${_AOTS_TT_VALUE};"><strong>${_mapT('Name')}:</strong> ${facility_name}</div>` : ''}
        ${facility_type ? `<div style="font-size: 11px; color: ${_AOTS_TT_LABEL};"><strong>${_mapT('Type')}:</strong> ${facility_type}</div>` : ''}
    `;
    if (probability !== undefined && probability !== null) {
        content += `<div style="font-size: 11px; color: ${_AOTS_TT_VALUE};"><strong>${_mapT('Impact Probability')}:</strong> ${formatPercent(probability)}</div>`;
    } else {
        content += `<div style="font-size: 11px; color: ${_AOTS_TT_SUB}; font-style: italic;">${_mapT('Base location (no impact data)')}</div>`;
    }

    layer.bindTooltip(content, {sticky: true});
}
""")

tooltip_shelters = assign("""
function(feature, layer) {
    const props = feature.properties || {};
    const escapeHtml = (s) => {
        if (typeof s !== 'string') return s;
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#x27;');
    };
    const name = escapeHtml(props.name || props.name_en || null);
    const shelter_type = escapeHtml(props.shelter_type || props.type || null);
    const category = escapeHtml(props.category || null);
    const probability = props.probability;

    const formatPercent = (prob) => {
        if (typeof prob === 'number') return (prob * 100).toFixed(1) + '%';
        return 'N/A';
    };

    let content = `
        <div style="font-size: 13px; font-weight: 600; color: #FF8C00; margin-bottom: 5px;">
            ${_mapT('Shelter')}
        </div>
    `;
    if (name) content += `<div style="font-size: 11px; color: ${_AOTS_TT_VALUE};"><strong>${_mapT('Name')}:</strong> ${name}</div>`;
    if (shelter_type) content += `<div style="font-size: 11px; color: ${_AOTS_TT_LABEL};"><strong>${_mapT('Type')}:</strong> ${shelter_type}</div>`;
    if (category) content += `<div style="font-size: 11px; color: ${_AOTS_TT_LABEL};"><strong>${_mapT('Category')}:</strong> ${category}</div>`;
    if (probability !== undefined && probability !== null) {
        content += `<div style="font-size: 11px; color: ${_AOTS_TT_VALUE};"><strong>${_mapT('Impact Probability')}:</strong> ${formatPercent(probability)}</div>`;
    } else {
        content += `<div style="font-size: 11px; color: ${_AOTS_TT_SUB}; font-style: italic;">${_mapT('Base location (no impact data)')}</div>`;
    }

    layer.bindTooltip(content, {sticky: true});
}
""")

tooltip_wash = assign("""
function(feature, layer) {
    const props = feature.properties || {};
    const escapeHtml = (s) => {
        if (typeof s !== 'string') return s;
        return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#x27;');
    };
    const name = escapeHtml(props.name || props.name_en || null);
    const wash_type = escapeHtml(props.wash_type || props.type || null);
    const category = escapeHtml(props.category || null);
    const probability = props.probability;

    const formatPercent = (prob) => {
        if (typeof prob === 'number') return (prob * 100).toFixed(1) + '%';
        return 'N/A';
    };

    let content = `
        <div style="font-size: 13px; font-weight: 600; color: #008B8B; margin-bottom: 5px;">
            ${_mapT('WASH Facility')}
        </div>
    `;
    if (name) content += `<div style="font-size: 11px; color: ${_AOTS_TT_VALUE};"><strong>${_mapT('Name')}:</strong> ${name}</div>`;
    if (wash_type) content += `<div style="font-size: 11px; color: ${_AOTS_TT_LABEL};"><strong>${_mapT('Type')}:</strong> ${wash_type}</div>`;
    if (category) content += `<div style="font-size: 11px; color: ${_AOTS_TT_LABEL};"><strong>${_mapT('Category')}:</strong> ${category}</div>`;
    if (probability !== undefined && probability !== null) {
        content += `<div style="font-size: 11px; color: ${_AOTS_TT_VALUE};"><strong>${_mapT('Impact Probability')}:</strong> ${formatPercent(probability)}</div>`;
    } else {
        content += `<div style="font-size: 11px; color: ${_AOTS_TT_SUB}; font-style: italic;">${_mapT('Base location (no impact data)')}</div>`;
    }

    layer.bindTooltip(content, {sticky: true});
}
""")

# tooltip_tiles and tooltip_admin removed: see note above (MapLibre handles tile/admin tooltips)

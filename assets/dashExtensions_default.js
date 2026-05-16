window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(e) {
                var lMap = e.target;
                window._leaflet_maps = window._leaflet_maps || {};
                window._leaflet_maps['main-map'] = lMap;
                window._aots_leaflet_ready_map = lMap;
                if (window._aots_maplibre && window._aots_maplibre_ready) {
                    var c = lMap.getCenter();
                    window._aots_maplibre.jumpTo({
                        center: [c.lng, c.lat],
                        zoom: lMap.getZoom() - 1
                    });
                }
            }

            ,
        function1: function(e) {
                var lMap = e.target;
                window._leaflet_maps = window._leaflet_maps || {};
                window._leaflet_maps['main-map'] = lMap;
                if (window._aots_maplibre && window._aots_maplibre_ready) {
                    var c = lMap.getCenter();
                    window._aots_maplibre.jumpTo({
                        center: [c.lng, c.lat],
                        zoom: lMap.getZoom() - 1
                    });
                }
            }

            ,
        function2: function(feature, context) {
                const member_type = feature.properties?.member_type;
                if (member_type === 'control') {
                    return {
                        color: '#ff0000',
                        weight: 4,
                        opacity: 1.0
                    };
                } else {
                    return {
                        color: '#1cabe2',
                        weight: 2,
                        opacity: 0.8
                    };
                }
            }

            ,
        function3: function(feature, latlng, context) {
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

            ,
        function4: function(feature, context) {
                const props = feature.properties || {};
                const severity_population = props.severity_population || 0;
                const max_population = props.max_population || 1;
                const is_stacked = props.is_stacked || false;

                // Gray for no data or zero impact
                if (!severity_population || severity_population === 0) {
                    // Higher opacity for stacked envelopes
                    const baseOpacity = is_stacked ? 0.6 : 0.3;
                    return {
                        color: '#808080',
                        weight: 2,
                        fillColor: '#808080',
                        fillOpacity: baseOpacity
                    };
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

                // Interpolate between yellow (#FFFF00) and dark red (#8B0000)
                const color = interpolateColor('#FFFF00', '#8B0000', easedSeverity);

                // Opacity increases with severity
                // Stacked envelopes: make more transparent to reveal basemap/country layers beneath
                //   -> 0.15 to 0.50 range
                // Regular envelopes: 0.3 to 0.9 opacity range
                if (is_stacked) {
                    const fillOpacity = 0.15 + (easedSeverity * 0.35); // Range: 0.15 to 0.50
                    return {
                        color: color,
                        weight: 3,
                        fillColor: color,
                        fillOpacity: fillOpacity,
                        opacity: 0.6
                    };
                } else {
                    const fillOpacity = 0.3 + (easedSeverity * 0.6); // Range: 0.3 to 0.9 (original)
                    return {
                        color: color,
                        weight: 2,
                        fillColor: color,
                        fillOpacity: fillOpacity
                    };
                }
            }

            ,
        function5: function(feature, layer) {
                const props = feature.properties || {};
                const escapeHtml = (s) => {
                    if (typeof s !== 'string') return s;
                    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#x27;');
                };
                const member_raw = props.ensemble_member;
                const member = member_raw != null ? escapeHtml(String(member_raw)) : null;
                const type = props.member_type || 'N/A';

                const label = type === 'control' ? 'Control Track' : 'Ensemble Track';

                const content = `
        <div style="font-size: 13px; font-weight: 600; color: #1cabe2; margin-bottom: 5px;">
            ${label}
        </div>
        <div style="font-size: 12px; color: #555;">
            <strong>Ensemble Member:</strong> ${member !== null ? '#' + member : 'N/A'}
        </div>
    `;

                layer.bindTooltip(content, {
                    sticky: true
                });
            }

            ,
        function6: function(feature, layer) {
                const props = feature.properties || {};
                const escapeHtml = (s) => {
                    if (typeof s !== 'string') return s;
                    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#x27;');
                };
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
            Hurricane Envelope
        </div>
        <div style="font-size: 12px; color: #555;">
            <strong>Wind Threshold:</strong> ${wind_threshold}
        </div>
        <div style="font-size: 12px; color: #555;">
            <strong>Ensemble Member:</strong> ${ensemble_member !== 'N/A' ? '#' + ensemble_member : 'N/A'}
        </div>
    `;

                // Children total: N/A only when all components are null; 0 when all are confirmed 0
                const _isNoData = v => v == null || (typeof v === 'number' && isNaN(v));
                const _sev_children_all_null = _isNoData(severity_infant_population) && _isNoData(severity_school_age_population) && _isNoData(severity_adolescent_population);
                const sev_children_total = _sev_children_all_null ? null : (severity_infant_population || 0) + (severity_school_age_population || 0) + (severity_adolescent_population || 0);

                content += `
        <hr style="margin: 5px 0; border: none; border-top: 1px solid #ddd;">
        <div style="font-size: 11px; color: #777; margin-top: 5px;">
            <strong>Impact:</strong>
        </div>
        <div style="font-size: 11px; color: #555;">
            Population: ${fmtImpact(severity_population)}
        </div>
        <div style="font-size: 11px; color: #555;">
            Children<span style="font-size: 0.85em; color: #888; margin-left: 3px;">(total)</span>: ${sev_children_total !== null ? fmtImpact(sev_children_total) : 'N/A'}
        </div>
        <div style="font-size: 10px; color: #888; padding-left: 10px; font-style: italic;">
            Age 0–4: ${fmtImpact(severity_infant_population)}
        </div>
        <div style="font-size: 10px; color: #888; padding-left: 10px; font-style: italic;">
            Age 5–14: ${fmtImpact(severity_school_age_population)}
        </div>
        <div style="font-size: 10px; color: #888; padding-left: 10px; font-style: italic;">
            Age 15–19: ${fmtImpact(severity_adolescent_population)}
        </div>
        <div style="font-size: 11px; color: #555;">
            Schools: ${fmtImpact(severity_schools)}
        </div>
        <div style="font-size: 11px; color: #555;">
            Health Centers: ${fmtImpact(severity_hcs)}
        </div>
        <div style="font-size: 11px; color: #555;">
            Shelters: ${fmtImpact(severity_num_shelters)}
        </div>
        <div style="font-size: 11px; color: #555;">
            WASH Facilities: ${fmtImpact(severity_num_wash)}
        </div>
        <div style="font-size: 11px; color: #555;">
            Built Surface: ${fmtSurface(severity_built_surface_m2)}
        </div>
    `;

                const _hasInNeed = v => v != null && typeof v === 'number' && !isNaN(v) && v > 0;
                if (_hasInNeed(severity_people_in_need) || _hasInNeed(severity_children_in_need)) {
                    const sev_chin_all_null = severity_infant_in_need == null && severity_school_age_in_need == null && severity_adolescent_in_need == null;
                    const sev_chin_total = sev_chin_all_null ? null : (severity_infant_in_need || 0) + (severity_school_age_in_need || 0) + (severity_adolescent_in_need || 0);
                    content += `
        <hr style="margin: 5px 0; border: none; border-top: 1px solid #ddd;">
        <div style="font-size: 11px; color: #f59f00; font-weight: 600; margin-top: 5px;">In Need:</div>
        <div style="font-size: 11px; color: #f59f00;">Population: ${_hasInNeed(severity_people_in_need) ? formatNumber(severity_people_in_need) : 'N/A'}</div>
        <div style="font-size: 11px; color: #f59f00;">Children<span style="font-size: 0.85em; margin-left: 3px;">(total)</span>: ${sev_chin_total !== null ? formatNumber(sev_chin_total) : (_hasInNeed(severity_children_in_need) ? formatNumber(severity_children_in_need) : 'N/A')}</div>
        <div style="font-size: 10px; color: #f5b942; padding-left: 10px; font-style: italic;">Age 0–4: ${_hasInNeed(severity_infant_in_need) ? formatNumber(severity_infant_in_need) : 'N/A'}</div>
        <div style="font-size: 10px; color: #f5b942; padding-left: 10px; font-style: italic;">Age 5–14: ${_hasInNeed(severity_school_age_in_need) ? formatNumber(severity_school_age_in_need) : 'N/A'}</div>
        <div style="font-size: 10px; color: #f5b942; padding-left: 10px; font-style: italic;">Age 15–19: ${_hasInNeed(severity_adolescent_in_need) ? formatNumber(severity_adolescent_in_need) : 'N/A'}</div>
        `;
                }

                layer.bindTooltip(content, {
                    sticky: true
                });
            }

            ,
        function7: function(feature, layer) {
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
            School
        </div>
        ${school_name ? `<div style="font-size: 12px; color: #555;"><strong>Name:</strong> ${school_name}</div>` : ''}
    `;
                if (probability !== undefined && probability !== null) {
                    content += `<div style="font-size: 12px; color: #555;"><strong>Impact Probability:</strong> ${formatPercent(probability)}</div>`;
                } else {
                    content += `<div style="font-size: 11px; color: #888; font-style: italic;">Base location (no impact data)</div>`;
                }

                layer.bindTooltip(content, {
                    sticky: true
                });
            }

            ,
        function8: function(feature, layer) {
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
            Health Facility
        </div>
        ${facility_name ? `<div style="font-size: 12px; color: #555;"><strong>Name:</strong> ${facility_name}</div>` : ''}
        ${facility_type ? `<div style="font-size: 11px; color: #777;"><strong>Type:</strong> ${facility_type}</div>` : ''}
    `;
                if (probability !== undefined && probability !== null) {
                    content += `<div style="font-size: 12px; color: #555;"><strong>Impact Probability:</strong> ${formatPercent(probability)}</div>`;
                } else {
                    content += `<div style="font-size: 11px; color: #888; font-style: italic;">Base location (no impact data)</div>`;
                }

                layer.bindTooltip(content, {
                    sticky: true
                });
            }

            ,
        function9: function(feature, layer) {
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
            Shelter
        </div>
    `;
                if (name) content += `<div style="font-size: 12px; color: #555;"><strong>Name:</strong> ${name}</div>`;
                if (shelter_type) content += `<div style="font-size: 11px; color: #777;"><strong>Type:</strong> ${shelter_type}</div>`;
                if (category) content += `<div style="font-size: 11px; color: #777;"><strong>Category:</strong> ${category}</div>`;
                if (probability !== undefined && probability !== null) {
                    content += `<div style="font-size: 12px; color: #555;"><strong>Impact Probability:</strong> ${formatPercent(probability)}</div>`;
                } else {
                    content += `<div style="font-size: 11px; color: #888; font-style: italic;">Base location (no impact data)</div>`;
                }

                layer.bindTooltip(content, {
                    sticky: true
                });
            }

            ,
        function10: function(feature, layer) {
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
            WASH Facility
        </div>
    `;
            if (name) content += `<div style="font-size: 12px; color: #555;"><strong>Name:</strong> ${name}</div>`;
            if (wash_type) content += `<div style="font-size: 11px; color: #777;"><strong>Type:</strong> ${wash_type}</div>`;
            if (category) content += `<div style="font-size: 11px; color: #777;"><strong>Category:</strong> ${category}</div>`;
            if (probability !== undefined && probability !== null) {
                content += `<div style="font-size: 12px; color: #555;"><strong>Impact Probability:</strong> ${formatPercent(probability)}</div>`;
            } else {
                content += `<div style="font-size: 11px; color: #888; font-style: italic;">Base location (no impact data)</div>`;
            }

            layer.bindTooltip(content, {
                sticky: true
            });
        }

    }
});
window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(feature, context) {
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
        function1: function(feature, context) {
                const props = feature.properties || {};
                const color = props._color || '#808080';
                const fillOpacity = props._fillOpacity || 0.7;
                const weight = props._weight || 1;
                const opacity = props._opacity || 0.8;

                return {
                    color: color,
                    weight: weight,
                    opacity: opacity,
                    fillColor: color,
                    fillOpacity: fillOpacity
                };
            }

            ,
        function2: function(feature, latlng, context) {
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
        function3: function(feature, context) {
                const props = feature.properties || {};
                const severity_population = props.severity_population || 0;
                const max_population = props.max_population || 1;

                // Gray for no data or zero impact
                if (!severity_population || severity_population === 0) {
                    return {
                        color: '#808080',
                        weight: 2,
                        fillColor: '#808080',
                        fillOpacity: 0.3
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

                // Opacity also increases with severity
                const fillOpacity = 0.3 + (easedSeverity * 0.6);

                return {
                    color: color,
                    weight: 2,
                    fillColor: color,
                    fillOpacity: fillOpacity
                };
            }

            ,
        function4: function(feature, layer) {
                const props = feature.properties || {};
                const member = props.ensemble_member || 'N/A';
                const type = props.member_type || 'N/A';

                const label = type === 'control' ? 'Control Track' : 'Ensemble Track';

                const content = `
        <div style="font-size: 13px; font-weight: 600; color: #1cabe2; margin-bottom: 5px;">
            ${label}
        </div>
        <div style="font-size: 12px; color: #555;">
            <strong>Ensemble Member:</strong> #${member}
        </div>
    `;

                layer.bindTooltip(content, {
                    sticky: true
                });
            }

            ,
        function5: function(feature, layer) {
                const props = feature.properties || {};
                const wind_threshold = props.wind_threshold || props.WIND_THRESHOLD || 'N/A';
                const ensemble_member = props.ensemble_member || props.ENSEMBLE_MEMBER || 'N/A';
                const severity_school_age_population = props.severity_school_age_population || 0;
                const severity_infant_population = props.severity_infant_population || 0;
                const severity_population = props.severity_population || 0;
                const severity_schools = props.severity_schools || 0;
                const severity_hcs = props.severity_hcs || 0;
                const severity_built_surface_m2 = props.severity_built_surface_m2 || 0;


                const formatNumber = (num) => {
                    if (typeof num === 'number') {
                        return new Intl.NumberFormat('en-US').format(Math.round(num));
                    }
                    return num;
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

                // Always show impact section
                content += `
        <hr style="margin: 5px 0; border: none; border-top: 1px solid #ddd;">
        <div style="font-size: 11px; color: #777; margin-top: 5px;">
            <strong>Impact:</strong>
        </div>
        <div style="font-size: 11px; color: #555;">
            Population: ${severity_population > 0 ? formatNumber(severity_population) : 'N/A'}
        </div>
        <div style="font-size: 11px; color: #555; padding-left: 10px; font-style: italic;">
            Age 5-15: ${severity_school_age_population > 0 ? formatNumber(severity_school_age_population) : 'N/A'}
        </div>
        <div style="font-size: 11px; color: #555; padding-left: 10px; font-style: italic;">
            Age 0-5: ${severity_infant_population > 0 ? formatNumber(severity_infant_population) : 'N/A'}
        </div>
        <div style="font-size: 11px; color: #555;">
            Schools: ${severity_schools > 0 ? formatNumber(severity_schools) : 'N/A'}
        </div>
        <div style="font-size: 11px; color: #555;">
            Health Centers: ${severity_hcs > 0 ? formatNumber(severity_hcs) : 'N/A'}
        </div>
        <div style="font-size: 11px; color: #555;">
            Built Surface: ${severity_built_surface_m2 > 0 ? formatNumber(severity_built_surface_m2) + ' m²' : 'N/A'}
        </div>
    `;

                layer.bindTooltip(content, {
                    sticky: true
                });
            }

            ,
        function6: function(feature, layer) {
                const props = feature.properties || {};
                const probability = props.probability || 0;
                const school_id = props.school_id_giga || props.school_id || 'N/A';
                const school_name = props.school_name || props.name || props.school || 'N/A';

                const formatPercent = (prob) => {
                    if (typeof prob === 'number') {
                        return (prob * 100).toFixed(1) + '%';
                    }
                    return 'N/A';
                };

                const content = `
        <div style="font-size: 13px; font-weight: 600; color: #4169E1; margin-bottom: 5px;">
            School
        </div>
        ${school_name !== 'N/A' ? `<div style="font-size: 12px; color: #555;"><strong>Name:</strong> ${school_name}</div>` : ''}
        <div style="font-size: 12px; color: #555;">
            <strong>Impact Probability:</strong> ${formatPercent(probability)}
        </div>
    `;

                layer.bindTooltip(content, {
                    sticky: true
                });
            }

            ,
        function7: function(feature, layer) {
                const props = feature.properties || {};
                const probability = props.probability || 0;
                const osm_id = props.osm_id || 'N/A';
                const facility_name = props.facility_name || props.name || props.amenity_name || 'N/A';
                const facility_type = props.facility_type || props.amenity_type || props.type || 'N/A';

                const formatPercent = (prob) => {
                    if (typeof prob === 'number') {
                        return (prob * 100).toFixed(1) + '%';
                    }
                    return 'N/A';
                };

                const content = `
        <div style="font-size: 13px; font-weight: 600; color: #228B22; margin-bottom: 5px;">
            Health Facility
        </div>
        ${facility_name !== 'N/A' ? `<div style="font-size: 12px; color: #555;"><strong>Name:</strong> ${facility_name}</div>` : ''}
        ${facility_type !== 'N/A' ? `<div style="font-size: 11px; color: #777;"><strong>Type:</strong> ${facility_type}</div>` : ''}
        <div style="font-size: 12px; color: #555;">
            <strong>Impact Probability:</strong> ${formatPercent(probability)}
        </div>
    `;

                layer.bindTooltip(content, {
                    sticky: true
                });
            }

            ,
        function8: function(feature, layer) {
                const props = feature.properties || {};

                const formatNumber = (num) => {
                    if (typeof num === 'number') {
                        return new Intl.NumberFormat('en-US').format(Math.round(num));
                    }
                    return num;
                };

                let content = `
        <div style="font-size: 13px; font-weight: 600; color: #4169E1; margin-bottom: 5px;">
            Tile Statistics
        </div>
    `;

                // Expected impact values (from hurricane envelopes)
                const E_population = props.E_population || props.expected_population || 0;
                const E_built_surface_m2 = props.E_built_surface_m2 || props.expected_built_surface || 0;
                const E_num_schools = props.E_num_schools || 0;
                const E_school_age_population = props.E_school_age_population || 0;
                const E_infant_population = props.E_infant_population || 0;
                const E_num_hcs = props.E_num_hcs || 0;
                const E_rwi = props.E_rwi || 0;
                const probability = props.probability || 0;

                // Base infrastructure values
                const population = props.population || 0;
                const built_surface = props.built_surface_m2 || 0;
                const num_schools = props.num_schools || 0;
                const school_age_pop = props.school_age_population || 0;
                const infant_pop = props.infant_population || 0;
                const num_hcs = props.num_hcs || 0;
                const rwi = props.rwi || 0;
                const smod_class = props.smod_class || 'N/A';

                // Settlement classification mapping (values are 0, 10, 20, 30)
                const getSettlementLabel = (classNum) => {
                    if (classNum === null || classNum === undefined || classNum === '' || Number(classNum) === 0) return 'No Data';
                    // Convert to number and normalize to 0-3 range (divide by 10 if needed)
                    const num = Number(classNum);
                    const normalized = parseInt(num >= 10 ? num / 10 : num);
                    if (normalized === 1) return 'Rural';
                    if (normalized === 2) return 'Urban Clusters';
                    if (normalized === 3) return 'Urban Centers';
                    return 'N/A';
                };

                // Formatting helper functions
                const formatValue = (val) => {
                    if (val === null || val === undefined || (typeof val === 'number' && isNaN(val))) return 'N/A';
                    if (typeof val === 'number') return formatNumber(val);
                    return val === '' ? 'N/A' : val;
                };

                const formatSettlement = (val) => {
                    if (val === null || val === undefined || val === '' || (typeof val === 'number' && isNaN(val))) return 'N/A';
                    if (typeof val === 'number') return getSettlementLabel(val);
                    return 'N/A';
                };

                const formatDecimal = (val) => {
                    if (val === null || val === undefined || (typeof val === 'number' && isNaN(val)) || val === '') return 'N/A';
                    return val.toFixed(2);
                };

                // Show expected impact if available
                if (probability > 0) {
                    content += `
        <div style="font-size: 11px; color: #dc143c; margin-top: 5px; font-weight: 600;">
            Expected Impact:
        </div>
        <div style="font-size: 11px; color: #555;">
            Hurricane Impact Probability: ${(probability * 100).toFixed(1)}%
        </div>
        <hr style="margin: 5px 0; border: none; border-top: 1px solid #ddd;">
        `;
                }

                // Show tile data - always show all fields
                content += `
    <div style="font-size: 11px; color: #777; margin-top: 5px;">
        <strong>Tile Base Data:</strong>
    </div>
    <div style="font-size: 11px; color: #555;">
        Population: ${formatValue(population)}
    </div>
    <div style="font-size: 11px; color: #555; padding-left: 10px; font-style: italic;">
        Age 5-15: ${formatValue(school_age_pop)}
    </div>
    <div style="font-size: 11px; color: #555; padding-left: 10px; font-style: italic;">
        Age 0-5: ${formatValue(infant_pop)}
    </div>
    <div style="font-size: 11px; color: #555;">
        Schools: ${formatValue(num_schools)}
    </div>
    <div style="font-size: 11px; color: #555;">
        Health Centers: ${formatValue(num_hcs)}
    </div>
    <div style="font-size: 11px; color: #555;">
        Built Surface: ${built_surface > 0 ? formatNumber(built_surface) + ' m²' : 'N/A'}
    </div>
    <div style="font-size: 11px; color: #555;">
        Settlement: ${formatSettlement(smod_class)}
    </div>
    <div style="font-size: 11px; color: #555;">
        Relative Wealth Index: ${formatDecimal(rwi)}
    </div>
    `;

                layer.bindTooltip(content, {
                    sticky: true
                });
            }

            ,
        function9: function() {
            return {
                weight: 3,
                color: '#e53935'
            };
        },
        function10: function(feature, layer) {
            const props = feature.properties || {};
            const rows = Object.keys(props).map(k =>
                `<tr><th style="text-align:left;padding-right:6px;">${k}</th><td>${props[k]}</td></tr>`
            ).join('');
            const html = `<div style="font-size:12px;"><table>${rows}</table></div>`;
            if (layer && layer.bindTooltip) {
                layer.bindTooltip(html, {
                    sticky: true
                });
            }
        }

    }
});
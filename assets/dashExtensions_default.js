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
        function2: function(feature, context) {
                const props = feature.properties || {};
                const color = props._color || '#808080';
                const radius = props._radius || 8;
                const opacity = props._opacity || 0.8;
                const weight = props._weight || 1;
                const fillOpacity = props._fillOpacity || 0.7;

                return {
                    color: color,
                    weight: weight,
                    opacity: opacity,
                    fillColor: color,
                    fillOpacity: fillOpacity,
                    radius: radius
                };
            }

            ,
        function3: function(feature, latlng, context) {
                const props = feature.properties || {};
                const color = props._color || '#808080';
                const radius = props._radius || 8;
                const opacity = props._opacity || 0.8;
                const weight = props._weight || 1;
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
        function5: function(feature, layer) {
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
        function6: function(feature, layer) {
                const props = feature.properties || {};
                const wind_threshold = props.wind_threshold || props.WIND_THRESHOLD || 'N/A';
                const ensemble_member = props.ensemble_member || props.ENSEMBLE_MEMBER || 'N/A';
                const severity_population = props.severity_population || 0;
                const severity_schools = props.severity_schools || 0;
                const severity_hcs = props.severity_hcs || 0;
                const severity_built_surface_m2 = props.severity_built_surface_m2 || 0;
                const severity_children = props.severity_children || 0;

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
            Children: ${severity_children > 0 ? formatNumber(severity_children) : 'N/A'}
        </div>
        <div style="font-size: 11px; color: #555;">
            Population: ${severity_population > 0 ? formatNumber(severity_population) : 'N/A'}
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
        function7: function() {
            return {
                weight: 3,
                color: '#e53935'
            };
        },
        function8: function(feature, layer) {
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
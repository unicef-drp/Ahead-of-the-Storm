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
                const wind_threshold = feature.properties?.wind_threshold;
                if (wind_threshold === 34) {
                    return {
                        color: '#ffff00',
                        weight: 2,
                        fillColor: '#ffff00',
                        fillOpacity: 0.3
                    };
                } else if (wind_threshold === 40) {
                    return {
                        color: '#ff8800',
                        weight: 2,
                        fillColor: '#ff8800',
                        fillOpacity: 0.3
                    };
                } else if (wind_threshold === 50) {
                    return {
                        color: '#ff0000',
                        weight: 2,
                        fillColor: '#ff0000',
                        fillOpacity: 0.3
                    };
                } else {
                    return {
                        color: '#888888',
                        weight: 2,
                        fillColor: '#888888',
                        fillOpacity: 0.3
                    };
                }
            }

            ,
        function5: function() {
            return {
                weight: 3,
                color: '#e53935'
            };
        },
        function6: function(feature, layer) {
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
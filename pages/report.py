import dash
import dash_mantine_components as dmc
from dash import Output, Input, State, callback, dcc, html, callback_context, ctx
import os
from jinja2 import Template
import json
import math
import base64
import io


from components.ui.header import make_header
from components.ui.footer import footer
from components.config import config
from components.data.data_store_utils import get_data_store

# Always use data store (works for both local filesystem and blob storage)
# For SPCS, this will be LocalDataStore pointing to /datastore
# For blob storage, this will be ADLSDataStore
giga_store = get_data_store()

# Get configuration from environment or config
RESULTS_DIR = config.RESULTS_DIR  # default: 'results' (set in config.py)
REPORT_TEMPLATE_FILE = os.getenv('REPORT_TEMPLATE_FILE', 'impact-report-template.html')

dash.register_page(
    __name__, path="/report", name="Impact Report"
)

################### Templates #####################
storm_categories = {34:'Tropical Storm',40:'Strong Tropical Storm',50:'Very Strong TS',
                    64:'Cat 1 Hurricane', 83:'Cat 2 Hurricane',96:'Cat 3 Hurricane',
                    113:'Cat 4 Hurricane', 137:'Cat 5 Hurricane'}

change_indicators = {'3':"<span class='change-indicator change-increase-large'>+{change}</span></td>",
           '2':"<span class='change-indicator change-increase-medium'>+{change}</span></td>",
           '1':"<span class='change-indicator change-increase-small'>+{change}</span></td>",
           '-3':"<span class='change-indicator change-decrease-large'>{change}</span></td>",
           '-2':"<span class='change-indicator change-decrease-medium'>{change}</span></td>",
           '-1':"<span class='change-indicator change-decrease-small'>{change}</span></td>",
}

row_admins_pop = """
            <tr>
                <td style="padding: 4px; font-weight: 600; max-width: 120px; word-wrap: break-word;">{admin_name}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_34} {change_pop_34}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_40} {change_pop_40}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_50} {change_pop_50}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_64} {change_pop_64}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_83} {change_pop_83}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_96} {change_pop_96}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_113} {change_pop_113}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_137} {change_pop_137}</td>
                <td style="text-align: center; padding: 4px; border-left: 2px solid #ccc;">{cci}</td>
            </tr>
"""

row_admins_pop_vulnerability = """
            <tr>
                <td style="padding: 4px; font-weight: 600; max-width: 120px; word-wrap: break-word;">{admin_name}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_poverty}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_severity}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_urban}</td>
                <td style="text-align: center; padding: 4px;">{expected_pop_rural}</td>
            </tr>
"""

row_poi_winds = """
            <tr>
                <td style="padding: 4px; font-weight: 600; max-width: 120px; word-wrap: break-word;">{admin_name}</td>
                <td style="text-align: center; padding: 4px;">{pois_34}</td>
                <td style="text-align: center; padding: 4px;">{pois_40}</td>
                <td style="text-align: center; padding: 4px;">{pois_50}</td>
                <td style="text-align: center; padding: 4px;">{pois_64}</td>
                <td style="text-align: center; padding: 4px;">{pois_83}</td>
                <td style="text-align: center; padding: 4px;">{pois_96}</td>
                <td style="text-align: center; padding: 4px;">{pois_113}</td>
                <td style="text-align: center; padding: 4px;">{pois_137}</td>
            </tr>
"""
###################################################

def format_change(value,key):
    if 'schools' in key or 'hcs' in key:
        abs_value = abs(value)
        for i in range(len(config.CHANGES_FACILITIES)):
            min,max = config.CHANGES_FACILITIES[i]
            if abs_value>=min and abs_value<max:
                if value>0:
                    index = f"{i+1}"
                else:
                    index = f"-{i+1}"
                return change_indicators[index].format(change=value)
    else:
        abs_value = abs(value)
        for i in range(len(config.CHANGES_POP)):
            min,max = config.CHANGES_POP[i]
            if abs_value>=min and abs_value<max:
                if value>0:
                    index = f"{i+1}"
                else:
                    index = f"-{i+1}"
                return change_indicators[index].format(change=value)
    return " "

def _fmt_count(val):
    """Format a count: 0 → light-grey span, non-zero → comma-separated integer.
    Floats are ceiling-rounded so 0.3 expected schools → 1, not 0."""
    if val is None:
        return '<span style="color: #ccc;">0</span>'
    if isinstance(val, float):
        val = math.ceil(val)
    if not isinstance(val, int):
        try:
            val = int(val)
        except (ValueError, TypeError):
            return str(val)
    if val == 0:
        return '<span style="color: #ccc;">0</span>'
    return f"{val:,}"

def refactor_html_str(html_str,d):
    d_refactored = d.copy()

    # Detect first forecast: no previous forecast to compare against.
    # When children_change_perc is missing/null/'-', change values equal population values — suppress all indicators.
    is_first_forecast = d.get('children_change_perc', '-') in ('-', '', None, False)
    d_refactored['is_first_forecast'] = is_first_forecast

    # Reformat global change indicators (suppressed on first forecast)
    for key in d.keys():
        if 'change' in key and key not in ["children_change_direction", "children_change", "children_change_perc"]:
            if is_first_forecast:
                d_refactored[key] = ' '
            else:
                d_refactored[key] = format_change(d[key], key)

    # Fix key name mismatch: JSON uses 'children_change_perc', template uses 'perc_children_change'
    perc = d.get('children_change_perc', '')
    if isinstance(perc, float):
        d_refactored['perc_children_change'] = f"{perc:.1f}"
    else:
        d_refactored['perc_children_change'] = str(perc) if perc else '-'

    # Format facility probabilities as percentages (e.g. 0.039 → 3.9%)
    for i in range(1, 6):
        for prefix in ['school_prob', 'hc_prob', 'shelter_prob', 'wash_prob']:
            key = f'{prefix}_{i}'
            val = d_refactored.get(key)
            if isinstance(val, float):
                d_refactored[key] = f"{val * 100:.1f}%"

    # Default missing _137 threshold keys to 0 (older pipeline JSONs omit them)
    for prefix in ['expected_children', 'expected_pop', 'expected_school', 'expected_infant',
                   'expected_adolescent', 'expected_schools', 'expected_hcs',
                   'change_children', 'change_school', 'change_infant', 'change_schools', 'change_hcs']:
        key = f'{prefix}_137'
        if key not in d_refactored:
            d_refactored[key] = 0
    # Suppress change formatting for _137 change keys (always 0 default, or first forecast)
    for prefix in ['change_children', 'change_school', 'change_infant', 'change_schools', 'change_hcs']:
        key = f'{prefix}_137'
        if d_refactored.get(key) == 0 or is_first_forecast:
            d_refactored[key] = ' '

    # Flags for "Most At Risk" sections with no data
    d_refactored['no_shelter_risk_data'] = not any(d.get(f'shelter_name_{i}') for i in range(1, 6))
    d_refactored['no_school_risk_data'] = not any(d.get(f'school_name_{i}') for i in range(1, 6))
    d_refactored['no_hc_risk_data'] = not any(d.get(f'hc_name_{i}') for i in range(1, 6))
    # WASH: has data if any name or non-zero probability exists
    wash_has_data = (any(d.get(f'wash_name_{i}') for i in range(1, 6)) or
                     any(isinstance(d.get(f'wash_prob_{i}'), float) and d[f'wash_prob_{i}'] > 0 for i in range(1, 6)))
    d_refactored['no_wash_risk_data'] = not wash_has_data

    # Replace empty facility names with "Name unknown"
    for i in range(1, 6):
        for prefix in ['school_name', 'hc_name', 'wash_name']:
            if not d_refactored.get(f'{prefix}_{i}'):
                d_refactored[f'{prefix}_{i}'] = 'Name unknown'

    # Format all numeric count fields — int or float (grey zero, comma-separated ceiling-rounded integer)
    for key, val in list(d_refactored.items()):
        if isinstance(val, (int, float)) and not isinstance(val, bool) and key.startswith('expected_'):
            d_refactored[key] = _fmt_count(val)

    # Compute total population change at the main threshold (sum of per-admin changes)
    # Used by template bullet: "Impact estimates changed by X people (Y children)"
    main_children = d.get('expected_children', 0)
    main_threshold = 34
    for wt in [34, 40, 50, 64, 83, 96, 113, 137]:
        if d.get(f'expected_children_{wt}') == main_children and main_children > 0:
            main_threshold = wt
            break
    if is_first_forecast:
        # First forecast: pop change equals the current value
        raw_pop_change = d.get(f'expected_pop_{main_threshold}', d.get('expected_pop', 0))
        if isinstance(raw_pop_change, (int, float)) and raw_pop_change > 0:
            d_refactored['pop_change'] = f"+{math.ceil(raw_pop_change):,}"
        else:
            d_refactored['pop_change'] = '0'
    else:
        raw_pop_change = sum(
            admin_d.get(f'change_{main_threshold}', 0)
            for admin_d in d.get('rows_admins_pop_total', [])
        )
        if raw_pop_change > 0:
            d_refactored['pop_change'] = f"+{raw_pop_change:,}"
        elif raw_pop_change < 0:
            d_refactored['pop_change'] = f"{raw_pop_change:,}"
        else:
            d_refactored['pop_change'] = '0'

    # expected_children is now the total of all age groups (0–4 + 5–14 + 15–19)
    raw_children = d.get('expected_children', 0)
    if isinstance(raw_children, (int, float)):
        d_refactored['expected_children_u24'] = _fmt_count(math.ceil(raw_children))
    else:
        d_refactored['expected_children_u24'] = d_refactored.get('expected_children', '0')

    # Format children_change string (e.g. '+39957' → '+39,957')
    cc = d_refactored.get('children_change', '')
    if isinstance(cc, str) and len(cc) > 1 and cc[0] in ('+', '-'):
        try:
            d_refactored['children_change'] = f"{cc[0]}{int(cc[1:]):,}"
        except ValueError:
            pass
    elif isinstance(cc, int):
        d_refactored['children_change'] = f"{cc:,}"

    #Reformat admins
    rows_admins_pop_total = []
    for admin_d in d_refactored['rows_admins_pop_total']:
        values = {'admin_name':admin_d['name'],'cci':_fmt_count(admin_d['cci'])}
        for wind in storm_categories.keys():
            values[f"expected_pop_{wind}"] = _fmt_count(admin_d[f'{wind}'])
            values[f"change_pop_{wind}"] = ' ' if is_first_forecast else format_change(admin_d[f"change_{wind}"],"")
        rows_admins_pop_total.append(row_admins_pop.format(**values))
    d_refactored['rows_admins_pop_total'] = "\n".join(rows_admins_pop_total)+"\n"

    rows_admins_school = []
    for admin_d in d_refactored['rows_admins_school']:
        values = {'admin_name':admin_d['name'],'cci':_fmt_count(admin_d['cci'])}
        for wind in storm_categories.keys():
            values[f"expected_pop_{wind}"] = _fmt_count(admin_d[f'{wind}'])
            values[f"change_pop_{wind}"] = ' ' if is_first_forecast else format_change(admin_d[f"change_{wind}"],"")
        rows_admins_school.append(row_admins_pop.format(**values))
    d_refactored['rows_admins_school'] = "\n".join(rows_admins_school)+"\n"

    rows_admins_infant = []
    for admin_d in d_refactored['rows_admins_infant']:
        values = {'admin_name':admin_d['name'],'cci':_fmt_count(admin_d['cci'])}
        for wind in storm_categories.keys():
            values[f"expected_pop_{wind}"] = _fmt_count(admin_d[f'{wind}'])
            values[f"change_pop_{wind}"] = ' ' if is_first_forecast else format_change(admin_d[f"change_{wind}"],"")
        rows_admins_infant.append(row_admins_pop.format(**values))
    d_refactored['rows_admins_infant'] = "\n".join(rows_admins_infant)+"\n"

    rows_schools_winds = []
    for admin_d in d_refactored['rows_schools_winds']:
        values = {'admin_name':admin_d['name']}
        for wind in storm_categories.keys():
            values[f"pois_{wind}"] = _fmt_count(admin_d[f'{wind}'])
        rows_schools_winds.append(row_poi_winds.format(**values))
    d_refactored['rows_schools_winds'] = "\n".join(rows_schools_winds)+"\n"

    rows_hcs_winds = []
    for admin_d in d_refactored['rows_hcs_winds']:
        values = {'admin_name':admin_d['name']}
        for wind in storm_categories.keys():
            values[f"pois_{wind}"] = _fmt_count(admin_d[f'{wind}'])
        rows_hcs_winds.append(row_poi_winds.format(**values))
    d_refactored['rows_hcs_winds'] = "\n".join(rows_hcs_winds)+"\n"

    # Age 15–19 admin rows (no change keys in this list)
    rows_admins_adolescent = []
    for admin_d in d_refactored.get('rows_admins_adolescent', []):
        values = {'admin_name':admin_d['name'],'cci':_fmt_count(admin_d['cci'])}
        for wind in storm_categories.keys():
            values[f"expected_pop_{wind}"] = _fmt_count(admin_d.get(str(wind), 0))
            values[f"change_pop_{wind}"] = ' '
        rows_admins_adolescent.append(row_admins_pop.format(**values))
    d_refactored['rows_admins_adolescent'] = "\n".join(rows_admins_adolescent)+"\n"

    rows_shelters_winds = []
    for admin_d in d_refactored.get('rows_shelters_winds', []):
        values = {'admin_name':admin_d['name']}
        for wind in storm_categories.keys():
            values[f"pois_{wind}"] = _fmt_count(admin_d[f'{wind}'])
        rows_shelters_winds.append(row_poi_winds.format(**values))
    d_refactored['rows_shelters_winds'] = "\n".join(rows_shelters_winds)+"\n"

    rows_wash_winds = []
    for admin_d in d_refactored.get('rows_wash_winds', []):
        values = {'admin_name':admin_d['name']}
        for wind in storm_categories.keys():
            values[f"pois_{wind}"] = _fmt_count(admin_d[f'{wind}'])
        rows_wash_winds.append(row_poi_winds.format(**values))
    d_refactored['rows_wash_winds'] = "\n".join(rows_wash_winds)+"\n"

    TEMPLATE = Template(html_str)
    html_str_new = TEMPLATE.render(d_refactored)

    return html_str_new


def _quadkey_to_latlon(qk):
    """Convert a quadkey string to the lat/lon centroid of that tile.

    Bing quadkey encoding: digit = x_bit + 2*y_bit, so:
      x bit comes from bit 0 of the digit (d & 1)
      y bit comes from bit 1 of the digit (d & 2)
    """
    zoom = len(qk)
    x = y = 0
    for i, c in enumerate(qk):
        mask = 1 << (zoom - 1 - i)
        d = int(c)
        if d & 1:
            x |= mask
        if d & 2:
            y |= mask
    n = 2 ** zoom
    lon = (x + 0.5) / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1.0 - 2.0 * (y + 0.5) / n)))
    lat = math.degrees(lat_rad)
    return lat, lon


def _generate_map_image(country, storm, forecast_date, wind_threshold=34):
    """
    Query MERCATOR_TILE_IMPACT_MAT for the given storm/date/threshold,
    convert quadkey ZONE_IDs to lat/lon, and return a base64-encoded PNG map.
    Returns None on any failure (map is optional).
    """
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        import matplotlib.colors as mcolors
        import numpy as np
        import contextily as ctx
        from pyproj import Transformer
        from components.data.snowflake_utils import get_snowflake_connection
    except ImportError as e:
        print(f"Impact report map: missing dependency — {e}")
        return None

    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
        sql = """
            SELECT ZONE_ID, PROBABILITY
            FROM AOTS.TC_ECMWF.MERCATOR_TILE_IMPACT_MAT
            WHERE COUNTRY = %(country)s
              AND STORM = %(storm)s
              AND FORECAST_DATE = %(forecast_date)s
              AND WIND_THRESHOLD = %(wind_threshold)s
              AND ZOOM_LEVEL = 14
              AND PROBABILITY > 0
        """
        cur.execute(sql, {
            'country': country,
            'storm': storm,
            'forecast_date': forecast_date,
            'wind_threshold': wind_threshold,
        })
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        print(f"Impact report map: error querying tile data — {e}")
        return None

    if not rows:
        print(f"Impact report map: no tile data found for {country}/{storm}/{forecast_date}/{wind_threshold}kt")
        return None

    # Convert quadkeys to lat/lon
    lats, lons, probs = [], [], []
    for zone_id, prob in rows:
        try:
            lat, lon = _quadkey_to_latlon(str(zone_id))
            lats.append(lat)
            lons.append(lon)
            probs.append(float(prob))
        except Exception:
            continue

    if not lats:
        return None

    try:
        # Project to Web Mercator for contextily
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        xs, ys = transformer.transform(lons, lats)

        _dpi = 250
        _fig_w, _fig_h = 14, 9
        from mpl_toolkits.axes_grid1 import make_axes_locatable
        fig, ax = plt.subplots(figsize=(_fig_w, _fig_h), dpi=_dpi)

        # Pad bounds before computing marker size so aspect ratio is final
        pad_x = (max(xs) - min(xs)) * 0.08 or 50000
        pad_y = (max(ys) - min(ys)) * 0.08 or 50000
        x_min, x_max = min(xs) - pad_x, max(xs) + pad_x
        y_min, y_max = min(ys) - pad_y, max(ys) + pad_y
        ax.set_xlim(x_min, x_max)
        ax.set_ylim(y_min, y_max)
        ax.set_aspect('equal')  # preserve geographic proportions

        # Compute marker size to approximately fill each zoom-14 tile (~2445 m/side).
        # s is in points² where 1 point = dpi/72 screen pixels.
        tile_m = 2 * math.pi * 6378137 / (2 ** 14)  # ~2445 m per tile side
        fig_w_px = _fig_w * _dpi
        px_per_m = fig_w_px / (x_max - x_min)
        tile_px = tile_m * px_per_m
        pts_per_px = 72 / _dpi
        marker_size = max((tile_px * pts_per_px) ** 2, 2.0)

        # Fixed 0–100% scale — low-probability storms stay yellow, not red.
        _dashboard_prob_colors = [
            '#ffffcc', '#ffeda0', '#fed976', '#feb24c', '#fd8d3c',
            '#fc4e2a', '#f03b20', '#e31a1c', '#bd0026', '#800026',
        ]
        cmap = mcolors.LinearSegmentedColormap.from_list('dashboard_prob', _dashboard_prob_colors)
        norm = mcolors.Normalize(vmin=0, vmax=1.0)

        # Add basemap FIRST so impact tiles render on top of it.
        # zoom_adjust=1 fetches higher-res tiles for a sharper basemap.
        mapbox_token = config.MAPBOX_ACCESS_TOKEN
        if mapbox_token:
            tile_url = (f"https://api.mapbox.com/styles/v1/mapbox/light-v11"
                        f"/tiles/256/{{z}}/{{x}}/{{y}}?access_token={mapbox_token}")
            try:
                ctx.add_basemap(ax, source=tile_url, attribution=False, zoom_adjust=1)
            except Exception as e:
                print(f"Impact report map: Mapbox tiles failed ({e}), falling back to CartoDB")
                ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, attribution=False,
                                zoom_adjust=1)
        else:
            ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, attribution=False,
                            zoom_adjust=1)

        probs_arr = np.array(probs)
        sc = ax.scatter(xs, ys, c=probs_arr, cmap=cmap, norm=norm,
                        s=marker_size, alpha=0.85, linewidths=0,
                        marker='s')

        ax.set_axis_off()

        # Wind threshold label — bottom-right corner of the map
        ax.text(0.98, 0.02, f'Wind speed threshold: {wind_threshold} kt',
                transform=ax.transAxes, ha='right', va='bottom',
                fontsize=9, color='#333333',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.85,
                          edgecolor='#cccccc', linewidth=0.5))

        # Horizontal colorbar below the map — appended via make_axes_locatable
        # so it never overlaps the map content.
        divider = make_axes_locatable(ax)
        cbar_ax = divider.append_axes('bottom', size='4%', pad=0.08)
        cb = plt.colorbar(sc, cax=cbar_ax, orientation='horizontal')
        cb.set_label('Probability of wind exceeding threshold', fontsize=9,
                     color='#333333', labelpad=4)
        cb.set_ticks([0, 0.25, 0.5, 0.75, 1.0])
        cb.set_ticklabels(['0%', '25%', '50%', '75%', '100%'])
        cb.ax.tick_params(labelsize=8, colors='#333333', length=3)
        cb.outline.set_edgecolor('#cccccc')
        cb.outline.set_linewidth(0.5)

        plt.tight_layout(pad=0.3)

        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', dpi=_dpi)
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode('utf-8')

    except Exception as e:
        print(f"Impact report map: error generating figure — {e}")
        return None


def make_custom_header():
    """Use the standard header which now includes Last Updated timestamp"""
    from components.ui.header import make_header
    return make_header(active_tab="tab-report")

def _load_template():
    """Load report HTML template from the data store. Returns html_str or None."""
    report_path = os.path.join(RESULTS_DIR, REPORT_TEMPLATE_FILE)
    if giga_store.file_exists(report_path):
        print(f"Impact report: Loading template from {report_path}")
        with giga_store.open(report_path, 'r') as f:
            return f.read()
    print(f"Impact report: Template not found at {report_path}")
    return None


def make_single_page_layout():
    html_str = _load_template()
    if not html_str:
        return dmc.Alert(
            f"Impact report template not found in data store at {os.path.join(RESULTS_DIR, REPORT_TEMPLATE_FILE)}.",
            title="Report Not Available",
            color="blue",
            variant="light"
        )

    return html.Iframe(
        srcDoc=html_str,
        style={"width": "100%", "height": "100%", "border": 0},
        id='iframe'
    )

def make_single_page_appshell():
    """Create appshell with custom header using existing footer and appshell structure"""
    from components.ui.footer import footer
    
    return dmc.AppShell(
        [
            dmc.AppShellHeader(make_custom_header(), px=15, zIndex=2000),
            dmc.AppShellMain(
                make_single_page_layout(),
                style={"height": "calc(100vh - 67px - 80px)", "overflow": "hidden", "display": "flex", "flexDirection": "column"}
            ),
            dmc.AppShellFooter(footer, zIndex=2000),
        ],
        id="single-page-shell",
        header={"height": "67"},
        padding=0,  # Remove all padding
        footer={"height": "80"},
    )

# Use the single-page appshell
layout = make_single_page_appshell()

_REGION_WARNING_HTML = """
<!DOCTYPE html><html><body style="font-family:sans-serif;padding:2rem;color:#333">
<div style="max-width:480px;margin:4rem auto;padding:1.5rem 2rem;border-left:4px solid #1cabe2;background:#f0f8ff;border-radius:4px">
  <h3 style="margin-top:0;color:#1cabe2">Report not available for regions</h3>
  <p>Impact reports can only be generated for individual countries.<br>
  Please select a specific country from the <strong>Individual country</strong> selector on the dashboard.</p>
</div></body></html>
"""

@callback(
    Output("iframe", "srcDoc"),
    Input("country-store","data"),
    Input("storm-store","data"),
    Input("date-store","data"),
    Input("country-is-region-store","data"),
    State("country-store","data"),
    State("storm-store","data"),
    State("date-store","data"),
    #prevent_initial_call=True
)
def update_iframe(i_country,i_storm,i_date,i_is_region,s_country,s_storm,s_date):
    if i_is_region:
        return _REGION_WARNING_HTML

    if s_country and s_storm and s_date:
        file = f"{s_country}_{s_storm}_{s_date}.json"
        filename = os.path.join(RESULTS_DIR, "jsons", file)
        
        print(f"Impact report: Loading {filename}")

        if not giga_store.file_exists(filename):
            print(f"Impact report: JSON not found at {filename}")
            return dash.no_update

        html_str = _load_template()
        if not html_str:
            print(f"Impact report: Template not found in data store — cannot render report")
            return dash.no_update

        # Read the JSON data file
        try:
            with giga_store.open(filename, 'r') as f:
                d = json.load(f)
        except Exception as e:
            print(f"Impact report: Error reading JSON {filename}: {e}")
            return dash.no_update

        # Find the main wind threshold: the threshold N where expected_children_N == expected_children
        main_children = d.get('expected_children', 0)
        map_threshold = 34  # fallback
        for wt in [34, 40, 50, 64, 83, 96, 113, 137]:
            if d.get(f'expected_children_{wt}') == main_children and main_children > 0:
                map_threshold = wt
                break

        # Generate static probability map (best-effort; report still renders without it)
        map_b64 = _generate_map_image(s_country, s_storm, s_date, wind_threshold=map_threshold)
        if map_b64:
            d['map_image'] = f'data:image/png;base64,{map_b64}'
        else:
            d['map_image'] = None

        try:
            return refactor_html_str(html_str, d)
        except Exception as e:
            print(f"Impact report: Error rendering report: {e}")
            return dash.no_update

    return dash.no_update
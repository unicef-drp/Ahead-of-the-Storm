-- ==============================================================================
-- 07b_alert_agent/01_map_udf.sql: Admin Choropleth PNG UDF
-- ==============================================================================
-- Deploy BEFORE 02_send_alert_procedure.sql: the procedure calls this UDF.
-- If the UDF is absent, map generation fails silently and the map is omitted.
-- Creates a Python UDF that renders an admin-level choropleth map as a
-- base64-encoded PNG, embedded directly in the email as an <img> tag.
--
-- Advantages over an inline SVG approach:
--   - Full geometry detail: no ST_SIMPLIFY, so offshore islands are preserved
--   - No topology gaps: matplotlib fills polygons cleanly at shared borders
--   - Universal email client support: PNG works everywhere; SVG does not
--   - Smaller payload: PNG is compressed; raw-geometry SVG would be ~1.7MB
--
-- Packages used (all available in Snowflake Anaconda channel):
--   matplotlib, shapely, numpy, pillow
--
-- Input:  JSON array of objects: [{name, geojson, children, clon, clat}, ...]
--         geojson: GeoJSON geometry string (Polygon or MultiPolygon)
--         children: expected children at risk (float, 0 = no impact)
--         clon/clat: centroid coordinates for label placement
-- Output: base64-encoded PNG string (embed as data:image/png;base64,...)
--
-- Called by: SEND_ALERT() in 02_send_alert_procedure.sql
--   SELECT AOTS.TC_ECMWF.GENERATE_ADMIN_MAP_PNG(<json>) AS png_b64
--
-- Fallback: if this UDF errors or returns NULL, the map section is omitted from the email.
-- ==============================================================================

USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;

CREATE OR REPLACE FUNCTION GENERATE_ADMIN_MAP_PNG(data_json VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('matplotlib', 'shapely', 'numpy', 'pillow')
HANDLER = 'run'
AS $$
import json, base64, io, math
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.colors as mcolors
import matplotlib.patheffects as pe
from matplotlib.patches import Polygon as MplPolygon
from matplotlib.collections import PatchCollection
from shapely.geometry import shape

# Dashboard color scale: light yellow → dark red (matches the web app)
HEX_COLORS = ['#ffffcc','#ffeda0','#fed976','#feb24c','#fd8d3c','#fc4e2a','#e31a1c','#bd0026','#800026']

def hex_to_rgb(h):
    h = h.lstrip('#')
    return tuple(int(h[i:i+2], 16) / 255.0 for i in (0, 2, 4))

RGB_COLORS = [hex_to_rgb(c) for c in HEX_COLORS]

def val_color(v, max_v):
    if not v or v == 0 or not max_v:
        return (1.0, 1.0, 1.0)  # white for zero
    t = v / max_v
    idx = min(int(t * len(RGB_COLORS)), len(RGB_COLORS) - 1)
    return RGB_COLORS[idx]

def fmt_n(n):
    return f"{int(round(n)):,}"


def run(data_json):
    try:
        data = json.loads(data_json)
        if not data:
            return None

        # Parse geometries
        features = []
        for item in data:
            if not item.get('geojson'):
                continue
            try:
                gj = json.loads(item['geojson'])
                geom = shape(gj)
                features.append({
                    'name': item.get('name', ''),
                    'geom': geom,
                    'children': float(item.get('children') or 0),
                    'clon': item.get('clon'),
                    'clat': item.get('clat'),
                })
            except Exception:
                continue

        if not features:
            return None

        max_val = max(f['children'] for f in features) or 1

        # Figure: 10×6 inches at 150dpi = 1500×900px (retina-quality for email)
        fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
        fig.patch.set_facecolor('#e8eef2')
        ax.set_facecolor('#e8eef2')

        # Fill pass: thin same-color edge bleeds into sub-pixel gaps between polygons.
        # Interior rings (holes) are painted with the background color to punch holes.
        for f in features:
            color = val_color(f['children'], max_val)
            geom = f['geom']
            polys = list(geom.geoms) if geom.geom_type == 'MultiPolygon' else [geom]
            for poly in polys:
                if poly.is_empty:
                    continue
                x, y = poly.exterior.xy
                ax.fill(x, y, facecolor=color, edgecolor=color, linewidth=0.6, antialiased=False)
                for interior in poly.interiors:
                    xi, yi = interior.xy
                    ax.fill(xi, yi, facecolor='#e8eef2', edgecolor='#e8eef2', linewidth=0.6, antialiased=False)

        # Outline pass: draw gray borders on top of the filled areas
        for f in features:
            geom = f['geom']
            polys = list(geom.geoms) if geom.geom_type == 'MultiPolygon' else [geom]
            for poly in polys:
                if poly.is_empty:
                    continue
                x, y = poly.exterior.xy
                ax.plot(x, y, color='#888888', linewidth=0.4, solid_capstyle='round', solid_joinstyle='round')

        # Wrap long names at the space nearest the midpoint, never truncate.
        def wrap_name(name, max_chars=12):
            if len(name) <= max_chars:
                return name
            mid = len(name) // 2
            best_pos, best_dist = None, len(name)
            for i, c in enumerate(name):
                if c == ' ':
                    dist = abs(i - mid)
                    if dist < best_dist:
                        best_dist = dist
                        best_pos = i
            if best_pos is not None:
                return name[:best_pos] + '\n' + name[best_pos + 1:]
            return name  # no space, single long word, show as-is

        # Labels: name + children count at centroid, white box background for legibility
        label_bbox = dict(boxstyle='round,pad=0.15', facecolor='white', alpha=0.75, edgecolor='none')
        for f in features:
            if f['clon'] is None or f['clat'] is None:
                continue
            label_name = wrap_name(f['name'])
            is_two_line = '\n' in label_name
            name_fontsize = 6.5 if is_two_line else 7
            name_offset  = 0.022 if is_two_line else 0.015
            label_val = fmt_n(f['children'])
            ax.text(f['clon'], f['clat'] + name_offset, label_name,
                    fontsize=name_fontsize, fontweight='bold', ha='center', va='bottom',
                    color='#111111', bbox=label_bbox)
            ax.text(f['clon'], f['clat'] - 0.010, label_val,
                    fontsize=6, ha='center', va='top',
                    color='#333333', bbox=label_bbox)

        # Bounding box from full geometry (includes all offshore islands)
        all_lons, all_lats = [], []
        for f in features:
            b = f['geom'].bounds  # (minx, miny, maxx, maxy)
            all_lons += [b[0], b[2]]
            all_lats += [b[1], b[3]]
        lon_range = max(all_lons) - min(all_lons)
        lat_range = max(all_lats) - min(all_lats)
        # Minimal padding so offshore islands aren't cropped
        pad_x = lon_range * 0.02
        pad_y = lat_range * 0.02
        ax.set_xlim(min(all_lons) - pad_x, max(all_lons) + pad_x)
        ax.set_ylim(min(all_lats) - pad_y, max(all_lats) + pad_y)

        # Cosine-corrected aspect ratio
        mid_lat = (min(all_lats) + max(all_lats)) / 2
        cos_lat = math.cos(math.radians(mid_lat))
        ax.set_aspect(1.0 / cos_lat)

        ax.axis('off')

        # Legend below map, use figure-level axes
        legend_patches = []
        n = len(HEX_COLORS)
        for i, c in enumerate(HEX_COLORS):
            lo = (i / n) * max_val
            hi = ((i + 1) / n) * max_val
            legend_patches.append(mpatches.Patch(
                facecolor=hex_to_rgb(c),
                edgecolor='#cccccc',
                linewidth=0.3,
                label=f"{fmt_n(lo)}–{fmt_n(hi)}"
            ))

        fig.legend(
            handles=legend_patches,
            title='Children at risk (0–19) at 50kt',
            title_fontsize=8,
            fontsize=7,
            loc='lower center',
            ncol=n,
            bbox_to_anchor=(0.5, 0),
            frameon=True,
            framealpha=0.9,
            edgecolor='#dddddd',
            handlelength=1.2,
            handleheight=0.9,
        )

        plt.tight_layout(rect=[0, 0.08, 1, 1])

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=150, bbox_inches='tight',
                    facecolor=fig.get_facecolor())
        plt.close(fig)
        buf.seek(0)
        return base64.b64encode(buf.read()).decode('utf-8')

    except Exception as e:
        return None  # map section omitted from email on failure
$$;

GRANT USAGE ON FUNCTION GENERATE_ADMIN_MAP_PNG(VARCHAR) TO ROLE AOTS_ROLE;
GRANT USAGE ON FUNCTION GENERATE_ADMIN_MAP_PNG(VARCHAR) TO ROLE SYSADMIN;

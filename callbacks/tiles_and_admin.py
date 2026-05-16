"""
Tile and admin layer callbacks.

Controls the Leaflet choropleth layers (raster tiles and admin polygons) that display
impact data on the map. Responsibilities:

- `juggle_toggles_*` — compute hideout dicts and disable radio options whenever the
  selected layer, probability toggle, or in-need switch changes. Both tile and admin
  variants delegate to `_compute_layer_toggle_outputs`.
- `toggle_probability_*` — independently update the probability hideout and its legend
  min/max labels from pre-computed stats stored in `tiles-stats-store` / `admin-stats-store`.
- `update_in_need_*_switches_disabled` — enable the "people in need" switches only when
  population/children-total is selected AND probability overlay is active AND vuln data exists.
- `toggle_tiles_legend` / `toggle_admin_legend` — show exactly one legend panel at a time,
  updating min/max labels from stats without iterating features.
- `_switch_styles` / `update_in_need_switch_styles` — colour-code the in-need toggle
  switches (green = at-risk active, blue = in-need active).
- `toggle_layer_mode` — swap visibility of the tiles vs. admin control panels.
- Infrastructure legend factory loop — show/hide per-layer infrastructure legends.
"""
import logging

import dash
from dash import Output, Input, State, callback

from components.config import config
from layouts.panels import _SWITCH_TRACK_BASE, _SWITCH_LABEL_BASE

logger = logging.getLogger(__name__)

# =============================================================================
# SECTION: CONSTANTS — LAYER PROPERTY MAPS
# Maps radio-group selection values to GeoJSON/stats property names. Used by
# toggle callbacks and load_all_layers (imported by dashboard.py).
# =============================================================================

# Maps radio-group selection values to the base (non-probabilistic) GeoJSON property name.
_LAYER_TO_PROP = {
    "population": "population", "children-total": "children_total",
    "infant": "infant_population", "school-age": "school_age_population",
    "adolescent": "adolescent_population", "built-surface": "built_surface_m2",
    "cci": config.CCI_COL, "settlement": "smod_class", "rwi": "rwi",
    "moderate-poverty": "moderate_poverty_prob", "severe-poverty": "severe_poverty_prob",
}
_LAYER_TO_E_PROP = {
    "population": "E_population", "children-total": "E_children_total",
    "infant": "E_infant_population", "school-age": "E_school_age_population",
    "adolescent": "E_adolescent_population", "built-surface": "E_built_surface_m2",
    "cci": config.E_CCI_COL, "settlement": "probability", "rwi": "probability",
    "moderate-poverty": "probability", "severe-poverty": "probability",
    "none": "probability", None: "probability",
}
_LAYER_TO_IN_NEED_PROP = {
    "population": "E_people_in_need",
    "children-total": "E_children_in_need",
}


# =============================================================================
# SECTION: CONSTANTS — DISPLAY AND STYLING
# Derived maps for probability overlay and in-need variants.
# =============================================================================

# Maps radio selection → expected-value property name for the probability overlay.
# For base-layer-only props (settlement, rwi) there is no E_ variant — fall back to raw probability.
_LAYER_TO_PROB_PROP = {
    "population":     "E_population",
    "children-total": "E_children_total",
    "infant":         "E_infant_population",
    "school-age":     "E_school_age_population",
    "adolescent":     "E_adolescent_population",
    "built-surface":  "E_built_surface_m2",
    "cci":            config.E_CCI_COL,
    "settlement":     None,
    "rwi":            None,
    "none":           "probability",
    None:             "probability",
}


# Reusable style dicts shared across legend toggle callbacks (extracted from
# toggle_tiles_legend / toggle_admin_legend to avoid 20+ local re-definitions).
_NONE = {"display": "none"}
_SHOW = {"display": "block"}


# =============================================================================
# SECTION: HELPERS
# Shared computation helpers used by multiple callbacks.
# =============================================================================

def _format_number(val):
    """Format a large number as a compact string (K/M suffix). Used in legend min/max labels."""
    if val >= 1_000_000:
        return f"{val / 1_000_000:.1f}M".replace('.0M', 'M')
    elif val >= 1_000:
        return f"{val / 1_000:.1f}K".replace('.0K', 'K')
    else:
        return f"{val:,.0f}"


def _compute_probability_outputs(prob_checked, selected_layer, stats):
    """Compute hideout dict + legend style + legend min/max for a probability overlay.

    Used by both the tiles and admin probability callbacks. Returns:
    (hideout, legend_style, min_label, max_label)
    """
    legend_style = {"display": "block"} if prob_checked else {"display": "none"}
    if not prob_checked:
        return {"hidden": True}, legend_style, "0%", "100%"

    property_name = _LAYER_TO_PROB_PROP.get(selected_layer, "probability")
    if property_name is None:
        property_name = "probability"

    hideout = {"prop": property_name}
    if stats and property_name in stats:
        hideout["min_val"] = stats[property_name].get("min")
        hideout["max_val"] = stats[property_name].get("max")

    min_val = "0"
    max_val = "100%"
    if property_name != "probability" and stats and property_name in stats:
        try:
            max_val = _format_number(stats[property_name]["max"])
        except Exception as e:
            logger.error(f"Error reading legend from stats: {e}")

    return hideout, legend_style, min_val, max_val


def _compute_layer_toggle_outputs(selected_layer, prob_checked, base_layers_only, in_need=False, stats=None):
    """Return hideout dicts and radio-option disabled flags shared by tile and admin layer callbacks.

    The 13-tuple returned maps directly onto the Outputs of juggle_toggles_tiles_layer and
    juggle_toggles_admin_layer: (pop_hideout, prob_hideout, pop_dis, chi_dis, inf_dis,
    sch_dis, ado_dis, blt_dis, cci_dis, set_dis, rwi_dis, mod_pov_dis, sev_pov_dis).

    Hideout dicts are consumed by the JS colourscale renderer; {"hidden": True} tells it
    to skip rendering. Stats are pre-computed in load_all_layers and stored in
    tiles-stats-store / admin-stats-store.
    """
    if in_need:
        pop_dis = chi_dis = False
        inf_dis = sch_dis = blt_dis = ado_dis = set_dis = rwi_dis = mod_pov_dis = sev_pov_dis = True
    elif prob_checked:
        pop_dis = inf_dis = sch_dis = blt_dis = ado_dis = chi_dis = False
        set_dis = rwi_dis = mod_pov_dis = sev_pov_dis = True
    else:
        pop_dis = inf_dis = sch_dis = blt_dis = ado_dis = chi_dis = False
        set_dis = rwi_dis = mod_pov_dis = sev_pov_dis = False
    cci_dis = bool(base_layers_only) or in_need

    pop_hidden = (not selected_layer or selected_layer == "none") or (
        prob_checked and not in_need and selected_layer in ["population", "children-total", "infant", "school-age", "adolescent", "built-surface", "cci"]
    )
    if in_need and selected_layer in _LAYER_TO_IN_NEED_PROP:
        prop = _LAYER_TO_IN_NEED_PROP[selected_layer]
    else:
        prop = _LAYER_TO_PROP.get(selected_layer, "population")
    e_prop = _LAYER_TO_E_PROP.get(selected_layer, "probability")
    def _h(p, stats):
        h = {"prop": p}
        if stats and p in stats:
            h["min_val"] = stats[p].get("min")
            h["max_val"] = stats[p].get("max")
        return h
    pop_hideout = {"hidden": True} if pop_hidden else _h(prop, stats)
    prob_hideout = _h(e_prop, stats) if (prob_checked and not in_need) else {"hidden": True}
    return pop_hideout, prob_hideout, pop_dis, chi_dis, inf_dis, sch_dis, ado_dis, blt_dis, cci_dis, set_dis, rwi_dis, mod_pov_dis, sev_pov_dis


# =============================================================================
# SECTION: TILE LAYER CALLBACKS — JUGGLE AND IN-NEED
# Control which radio options are enabled and compute hideout dicts for the tile
# raster layer.
# =============================================================================

@callback(
    Output("population-tiles-json", "hideout", allow_duplicate=True),
    Output("probability-tiles-json", "hideout", allow_duplicate=True),
    Output('population-tiles-layer', 'disabled', allow_duplicate=True),
    Output('children-total-tiles-layer', 'disabled', allow_duplicate=True),
    Output('infant-tiles-layer', 'disabled', allow_duplicate=True),
    Output('school-age-tiles-layer', 'disabled', allow_duplicate=True),
    Output('adolescent-tiles-layer', 'disabled', allow_duplicate=True),
    Output('built-surface-tiles-layer', 'disabled', allow_duplicate=True),
    Output('cci-tiles-layer', 'disabled', allow_duplicate=True),
    Output('settlement-tiles-layer', 'disabled', allow_duplicate=True),
    Output('rwi-tiles-layer', 'disabled', allow_duplicate=True),
    Output('moderate-poverty-tiles-layer', 'disabled', allow_duplicate=True),
    Output('severe-poverty-tiles-layer', 'disabled', allow_duplicate=True),
    Input('tiles-layer-group', 'value'),
    Input('probability-tiles-layer', 'checked'),
    Input('in-need-tiles-switch', 'checked'),
    Input('in-need-children-tiles-switch', 'checked'),
    State('using-base-layers-store', 'data'),
    State('tiles-stats-store', 'data'),
    prevent_initial_call=True,
)
def juggle_toggles_tiles_layer(selected_layer, prob_checked, in_need_pop, in_need_chi, base_layers_only, tiles_stats):
    """Recompute tile-layer hideouts and radio disabled flags on any control change."""
    in_need = bool(in_need_pop) if selected_layer == "population" else (bool(in_need_chi) if selected_layer == "children-total" else False)
    return _compute_layer_toggle_outputs(selected_layer, prob_checked, base_layers_only, in_need=in_need, stats=tiles_stats)


@callback(
    Output('in-need-tiles-switch', 'disabled', allow_duplicate=True),
    Output('in-need-children-tiles-switch', 'disabled', allow_duplicate=True),
    Input('tiles-layer-group', 'value'),
    Input('probability-tiles-layer', 'checked'),
    Input('tiles-stats-store', 'data'),
    prevent_initial_call=True,
)
def update_in_need_tiles_switches_disabled(layer, prob_checked, tiles_stats):
    """Enable the 'people in need' switch only when probability overlay is active and vuln data exists."""
    vuln_available = bool(tiles_stats) and 'E_people_in_need' in (tiles_stats or {})
    conditions_met = prob_checked and vuln_available
    return (
        not (layer == "population"      and conditions_met),
        not (layer == "children-total"  and conditions_met),
    )


# =============================================================================
# SECTION: ADMIN LAYER CALLBACKS — JUGGLE AND IN-NEED
# Same as tile layer callbacks but for admin polygon choropleth.
# =============================================================================

@callback(
    Output("population-admin-json", "hideout", allow_duplicate=True),
    Output("probability-admin-json", "hideout", allow_duplicate=True),
    Output('population-admin-layer', 'disabled', allow_duplicate=True),
    Output('children-total-admin-layer', 'disabled', allow_duplicate=True),
    Output('infant-admin-layer', 'disabled', allow_duplicate=True),
    Output('school-age-admin-layer', 'disabled', allow_duplicate=True),
    Output('adolescent-admin-layer', 'disabled', allow_duplicate=True),
    Output('built-surface-admin-layer', 'disabled', allow_duplicate=True),
    Output('cci-admin-layer', 'disabled', allow_duplicate=True),
    Output('settlement-admin-layer', 'disabled', allow_duplicate=True),
    Output('rwi-admin-layer', 'disabled', allow_duplicate=True),
    Output('moderate-poverty-admin-layer', 'disabled', allow_duplicate=True),
    Output('severe-poverty-admin-layer', 'disabled', allow_duplicate=True),
    Input('admin-layer-group', 'value'),
    Input('probability-admin-layer', 'checked'),
    Input('in-need-admin-switch', 'checked'),
    Input('in-need-children-admin-switch', 'checked'),
    State('using-base-layers-store', 'data'),
    State('admin-stats-store', 'data'),
    prevent_initial_call=True,
)
def juggle_toggles_admin_layer(selected_layer, prob_checked, in_need_pop, in_need_chi, base_layers_only, admin_stats):
    """Recompute admin-layer hideouts and radio disabled flags on any control change."""
    in_need = bool(in_need_pop) if selected_layer == "population" else (bool(in_need_chi) if selected_layer == "children-total" else False)
    return _compute_layer_toggle_outputs(selected_layer, prob_checked, base_layers_only, in_need=in_need, stats=admin_stats)


@callback(
    Output('in-need-admin-switch', 'disabled', allow_duplicate=True),
    Output('in-need-children-admin-switch', 'disabled', allow_duplicate=True),
    Input('admin-layer-group', 'value'),
    Input('probability-admin-layer', 'checked'),
    Input('admin-stats-store', 'data'),
    prevent_initial_call=True,
)
def update_in_need_admin_switches_disabled(layer, prob_checked, admin_stats):
    """Enable the 'people in need' switch only when probability overlay is active and vuln data exists."""
    vuln_available = bool(admin_stats) and 'E_people_in_need' in (admin_stats or {})
    conditions_met = prob_checked and vuln_available
    return (
        not (layer == "population"      and conditions_met),
        not (layer == "children-total"  and conditions_met),
    )


# =============================================================================
# SECTION: PROBABILITY OVERLAY CALLBACKS
# Handle the 'Impact Probability' checkbox that overlays expected-value data on
# tiles and admin layers.
# =============================================================================

@callback(
    Output("probability-tiles-json", "hideout", allow_duplicate=True),
    Output("probability-legend", "style", allow_duplicate=True),
    Output("probability-legend-min", "children", allow_duplicate=True),
    Output("probability-legend-max", "children", allow_duplicate=True),
    Input('probability-tiles-layer', 'checked'),
    Input('tiles-layer-group', 'value'),
    State('tiles-stats-store', 'data'),
    prevent_initial_call=True,
)
def toggle_probability_tiles_layer(prob_checked, selected_layer, tiles_stats):
    """Update probability overlay hideout and legend for the raster tile layer."""
    return _compute_probability_outputs(prob_checked, selected_layer, tiles_stats)


@callback(
    Output("probability-admin-json", "hideout", allow_duplicate=True),
    Output("probability-legend-admin", "style", allow_duplicate=True),
    Output("probability-legend-admin-min", "children", allow_duplicate=True),
    Output("probability-legend-admin-max", "children", allow_duplicate=True),
    Input('probability-admin-layer', 'checked'),
    Input('admin-layer-group', 'value'),
    State('admin-stats-store', 'data'),
    prevent_initial_call=True,
)
def toggle_probability_admin_layer(prob_checked, selected_layer, admin_stats):
    """Update probability overlay hideout and legend for the admin polygon layer."""
    return _compute_probability_outputs(prob_checked, selected_layer, admin_stats)


# =============================================================================
# SECTION: LAYER MODE
# Switch the control panel between 'Tiles (Raster)' and 'Admin Regions' modes.
# =============================================================================

@callback(
    [Output("tiles-mode-box", "style"),
     Output("admin-mode-box", "style")],
    Input("layer-mode-selector", "value"),
    prevent_initial_call=False
)
def toggle_layer_mode(selected_mode):
    """Show the tiles control panel or the admin regions panel, hiding the other."""
    if selected_mode == "tiles":
        return _SHOW, _NONE
    else:
        return _NONE, _SHOW


# =============================================================================
# SECTION: LEGEND VISIBILITY CALLBACKS
# Show/hide the correct legend panel based on the active layer selection and
# probability state.
# =============================================================================

@callback(
    [Output("population-legend", "style"),
     Output("population-in-need-legend", "style"),
     Output("children-total-legend", "style"),
     Output("children-in-need-legend", "style"),
     Output("infant-legend", "style"),
     Output("school-age-legend", "style"),
     Output("adolescent-legend", "style"),
     Output("built-surface-legend", "style"),
     Output("cci-legend", "style"),
     Output("settlement-legend", "style"),
     Output("rwi-legend", "style"),
     Output("moderate-poverty-legend", "style"),
     Output("severe-poverty-legend", "style"),
     Output("population-legend-min", "children"),
     Output("population-legend-max", "children"),
     Output("population-in-need-legend-min", "children"),
     Output("population-in-need-legend-max", "children"),
     Output("children-total-legend-min", "children"),
     Output("children-total-legend-max", "children"),
     Output("children-in-need-legend-min", "children"),
     Output("children-in-need-legend-max", "children"),
     Output("infant-legend-min", "children"),
     Output("infant-legend-max", "children"),
     Output("school-age-legend-min", "children"),
     Output("school-age-legend-max", "children"),
     Output("adolescent-legend-min", "children"),
     Output("adolescent-legend-max", "children"),
     Output("built-surface-legend-min", "children"),
     Output("built-surface-legend-max", "children"),
     Output("cci-legend-min", "children"),
     Output("cci-legend-max", "children"),
     ],
    [Input("tiles-layer-group", "value"),
     Input("probability-tiles-layer", "checked"),
     Input("in-need-tiles-switch", "checked"),
     Input("in-need-children-tiles-switch", "checked")],
    State("tiles-stats-store", "data"),
    prevent_initial_call=True
)
def toggle_tiles_legend(selected_value, prob_checked, in_need_pop, in_need_chi, tiles_stats):
    """Show the correct legend panel for the active tile layer.

    Logic: when in-need is active show the in-need legend (orange→brown) for pop/children;
    when probability overlay is on for a quantitative layer hide the per-layer legend (the
    probability legend takes over); otherwise show exactly the legend matching `selected_value`.
    The 13 style outputs map 1-to-1 to the Output list order; only one is _SHOW, the rest _NONE.
    The trailing 18 outputs are min/max label pairs read from pre-computed `tiles_stats`.
    """
    in_need = bool(in_need_pop) if selected_value == "population" else (bool(in_need_chi) if selected_value == "children-total" else False)

    def get_stats(prop):
        if tiles_stats and prop in tiles_stats:
            return _format_number(tiles_stats[prop]['min']), _format_number(tiles_stats[prop]['max'])
        return "Min", "Max"

    pop_min, pop_max                         = get_stats('population')
    pop_in_need_min, pop_in_need_max         = get_stats('E_people_in_need')
    chi_min, chi_max                         = get_stats('children_total')
    chi_in_need_min, chi_in_need_max         = get_stats('E_children_in_need')
    infant_min, infant_max                   = get_stats('infant_population')
    school_min, school_max                   = get_stats('school_age_population')
    adolescent_min, adolescent_max           = get_stats('adolescent_population')
    built_min, built_max                     = get_stats('built_surface_m2')
    cci_min, cci_max                         = get_stats(config.CCI_COL)

    _all_vals = (pop_min, pop_max, pop_in_need_min, pop_in_need_max, chi_min, chi_max, chi_in_need_min, chi_in_need_max, infant_min, infant_max, school_min, school_max, adolescent_min, adolescent_max, built_min, built_max, cci_min, cci_max)

    # 13 style outputs: pop, pop-in-need, chi, chi-in-need, infant, school, adolescent, built, cci, settlement, rwi, mod-pov, sev-pov
    _N = _NONE
    _S = _SHOW

    if in_need:
        if selected_value == "population":
            return _N, _S, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, *_all_vals
        elif selected_value == "children-total":
            return _N, _N, _N, _S, _N, _N, _N, _N, _N, _N, _N, _N, _N, *_all_vals
        return _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, *_all_vals

    if prob_checked and selected_value in ["population", "children-total", "infant", "school-age", "adolescent", "built-surface", "cci"]:
        return _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, *_all_vals

    if selected_value == "population":
        return _S, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, *_all_vals
    elif selected_value == "children-total":
        return _N, _N, _S, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, *_all_vals
    elif selected_value == "infant":
        return _N, _N, _N, _N, _S, _N, _N, _N, _N, _N, _N, _N, _N, *_all_vals
    elif selected_value == "school-age":
        return _N, _N, _N, _N, _N, _S, _N, _N, _N, _N, _N, _N, _N, *_all_vals
    elif selected_value == "adolescent":
        return _N, _N, _N, _N, _N, _N, _S, _N, _N, _N, _N, _N, _N, *_all_vals
    elif selected_value == "built-surface":
        return _N, _N, _N, _N, _N, _N, _N, _S, _N, _N, _N, _N, _N, *_all_vals
    elif selected_value == "cci":
        return _N, _N, _N, _N, _N, _N, _N, _N, _S, _N, _N, _N, _N, *_all_vals
    elif selected_value == "settlement":
        return _N, _N, _N, _N, _N, _N, _N, _N, _N, _S, _N, _N, _N, *_all_vals
    elif selected_value == "rwi":
        return _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _S, _N, _N, *_all_vals
    elif selected_value == "moderate-poverty":
        return _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _S, _N, *_all_vals
    elif selected_value == "severe-poverty":
        return _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _S, *_all_vals
    else:
        return _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, _N, *_all_vals


@callback(
    [Output("population-admin-legend", "style"),
     Output("children-total-admin-legend", "style"),
     Output("infant-admin-legend", "style"),
     Output("school-age-admin-legend", "style"),
     Output("adolescent-admin-legend", "style"),
     Output("built-surface-admin-legend", "style"),
     Output("cci-admin-legend", "style"),
     Output("settlement-admin-legend", "style"),
     Output("rwi-admin-legend", "style"),
     Output("moderate-poverty-admin-legend", "style"),
     Output("severe-poverty-admin-legend", "style"),
     Output("population-admin-legend-min", "children"),
     Output("population-admin-legend-max", "children"),
     Output("children-total-admin-legend-min", "children"),
     Output("children-total-admin-legend-max", "children"),
     Output("infant-admin-legend-min", "children"),
     Output("infant-admin-legend-max", "children"),
     Output("school-age-admin-legend-min", "children"),
     Output("school-age-admin-legend-max", "children"),
     Output("adolescent-admin-legend-min", "children"),
     Output("adolescent-admin-legend-max", "children"),
     Output("built-surface-admin-legend-min", "children"),
     Output("built-surface-admin-legend-max", "children"),
     Output("cci-admin-legend-min", "children"),
     Output("cci-admin-legend-max", "children"),
     ],
    [Input("admin-layer-group", "value"),
     Input("probability-admin-layer", "checked")],
    State("admin-stats-store", "data"),
    prevent_initial_call=True
)
def toggle_admin_legend(selected_value, prob_checked, admin_stats):
    """Show the correct legend panel for the active admin layer.

    Same pattern as toggle_tiles_legend but for admin polygon choropleth. Admin layers
    have no in-need switch, so the only special case is probability overlay suppression.
    The 11 style outputs + 14 min/max label outputs match the Output list order exactly.
    """
    def get_stats(prop):
        if admin_stats and prop in admin_stats:
            return _format_number(admin_stats[prop]['min']), _format_number(admin_stats[prop]['max'])
        return "Min", "Max"

    pop_min, pop_max                         = get_stats('population')
    children_total_min, children_total_max   = get_stats('children_total')
    infant_min, infant_max                   = get_stats('infant_population')
    school_min, school_max                   = get_stats('school_age_population')
    adolescent_min, adolescent_max           = get_stats('adolescent_population')
    built_min, built_max                     = get_stats('built_surface_m2')
    cci_min, cci_max                         = get_stats(config.CCI_COL)

    _all_vals = (pop_min, pop_max, children_total_min, children_total_max, infant_min, infant_max, school_min, school_max, adolescent_min, adolescent_max, built_min, built_max, cci_min, cci_max)

    if prob_checked and selected_value in ["population", "children-total", "infant", "school-age", "adolescent", "built-surface", "cci"]:
        return _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, *_all_vals

    if selected_value == "population":
        return _SHOW, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, *_all_vals
    elif selected_value == "children-total":
        return _NONE, _SHOW, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, *_all_vals
    elif selected_value == "infant":
        return _NONE, _NONE, _SHOW, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, *_all_vals
    elif selected_value == "school-age":
        return _NONE, _NONE, _NONE, _SHOW, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, *_all_vals
    elif selected_value == "adolescent":
        return _NONE, _NONE, _NONE, _NONE, _SHOW, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, *_all_vals
    elif selected_value == "built-surface":
        return _NONE, _NONE, _NONE, _NONE, _NONE, _SHOW, _NONE, _NONE, _NONE, _NONE, _NONE, *_all_vals
    elif selected_value == "cci":
        return _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _SHOW, _NONE, _NONE, _NONE, _NONE, *_all_vals
    elif selected_value == "settlement":
        return _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _SHOW, _NONE, _NONE, _NONE, *_all_vals
    elif selected_value == "rwi":
        return _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _SHOW, _NONE, _NONE, *_all_vals
    elif selected_value == "moderate-poverty":
        return _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _SHOW, _NONE, *_all_vals
    elif selected_value == "severe-poverty":
        return _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _SHOW, *_all_vals
    else:
        return _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, _NONE, *_all_vals


@callback(
    Output("in-need-tiles-note", "style"),
    Output("in-need-children-tiles-note", "style"),
    Input("in-need-tiles-switch", "checked"),
    Input("in-need-children-tiles-switch", "checked"),
    prevent_initial_call=True,
)
def toggle_in_need_notes(in_need_pop, in_need_chi):
    _base = {"fontSize": "0.72em", "fontWeight": 600, "color": "#5c7a9e", "backgroundColor": "#e8f0fb", "border": "1px solid #c5d8f5", "borderRadius": "6px", "padding": "5px 10px", "marginLeft": "12px", "marginTop": "4px", "marginBottom": "8px"}
    show = {**_base, "display": "block"}
    hide = {**_base, "display": "none"}
    return show if in_need_pop else hide, show if in_need_chi else hide


# Register one legend-visibility callback per infrastructure layer.
def _register_infra_legend_toggle(layer_id, legend_id):
    @callback(Output(legend_id, "style"), Input(f"{layer_id}-layer", "checked"), prevent_initial_call=True)
    def _toggle(checked):
        return {"display": "block" if checked else "none"}

for _lid, _lgd in [("schools", "schools-legend"), ("health", "health-legend"),
                   ("shelters", "shelters-infra-legend"), ("wash", "wash-infra-legend")]:
    _register_infra_legend_toggle(_lid, _lgd)


# =============================================================================
# SECTION: IN-NEED SWITCH STYLING
# Colour-code the 'People in Need' toggle switches: green when at-risk
# (probability on), blue when in-need is active.
# =============================================================================

def _switch_styles(checked, layer_active, prob_checked=False):
    """Return Mantine Switch `styles` dict for a single in-need toggle.

    Green track = at-risk mode active (prob on, not yet switched to in-need).
    Default blue = in-need is checked. Uncoloured = layer not active.
    """
    track = {**_SWITCH_TRACK_BASE}
    label = {**_SWITCH_LABEL_BASE}
    if layer_active and prob_checked and not checked:
        track["backgroundColor"] = "#48c774"
        track["borderColor"] = "#3aad62"
        label["color"] = "white"
    elif checked:
        label["color"] = "white"
    return {"track": track, "trackLabel": label}


@callback(
    Output('in-need-tiles-switch', 'styles'),
    Output('in-need-children-tiles-switch', 'styles'),
    Output('in-need-admin-switch', 'styles'),
    Output('in-need-children-admin-switch', 'styles'),
    Input('in-need-tiles-switch', 'checked'),
    Input('in-need-children-tiles-switch', 'checked'),
    Input('in-need-admin-switch', 'checked'),
    Input('in-need-children-admin-switch', 'checked'),
    Input('tiles-layer-group', 'value'),
    Input('admin-layer-group', 'value'),
    Input('probability-tiles-layer', 'checked'),
    Input('probability-admin-layer', 'checked'),
)
def update_in_need_switch_styles(pop_t, chi_t, pop_a, chi_a, tiles_layer, admin_layer, prob_tiles, prob_admin):
    """Return Mantine Switch styles for all four in-need toggles based on current layer and probability state."""
    return (
        _switch_styles(pop_t, tiles_layer == "population",     prob_tiles),
        _switch_styles(chi_t, tiles_layer == "children-total", prob_tiles),
        _switch_styles(pop_a, admin_layer == "population",     prob_admin),
        _switch_styles(chi_a, admin_layer == "children-total", prob_admin),
    )

"""
Standalone, print-friendly Full Impact Breakdown: companion to the main
dashboard's (pages/map_shell_concept.py) own Full Impact Breakdown modal.
That modal's "open in new tab" link points here instead of just reopening
the dashboard with the modal auto-opened on top of the full app chrome
(topbar/map/side panels), which isn't something you'd ever want to print or
hand someone as a link. This is just the report itself: a plain white page
with the forecast context (storm, forecast run, countries) up top, the
table, and the Admin Level 1 breakdowns below.

No dedicated Print button: the browser's own print (Cmd/Ctrl+P) already
covers that, and the page's @page CSS (assets/map_shell_breakdown_print.css)
already gives it sane margins.

Not linked from the header nav; reachable only via the modal's own link
(or directly, with the same ?zoom_countries=...&date=...&run=... query
params it generates). Reuses map_shell_concept's own builders
(_impact_breakdown_content, _t, _TRANSLATIONS, _STORMS, _cat_badge) rather
than re-implementing the table, so the printed report always matches
whatever the live modal currently shows.
"""
import dash
from dash import html
from datetime import datetime, timezone
from urllib.parse import unquote

dash.register_page(__name__, path="/breakdown-print", name="Full Impact Breakdown (Print)")


def layout(lang="en", zoom_countries=None, date=None, run=None,
            wind=None, river=None, rain=None, surge=None,
            windidx=None, riveridx=None, rainidx=None, surgeidx=None, rainwindow=None, riverwindow=None, **kwargs):
    # Deliberately imported HERE (request time), not at module level.
    # Dash's pages/ auto-loader (dash/_pages.py) walks the pages/ folder and
    # unconditionally exec_module()s every file with "register_page" in it;
    # it never checks sys.modules first, so it always executes
    # map_shell_concept.py exactly once during that walk regardless of
    # anything this file does. Because "map_shell_breakdown_print.py" sorts
    # alphabetically before "map_shell_concept.py", a module-level import of
    # map_shell_concept here would run Python's normal import machinery (a
    # genuine first execution, registering every @callback for real) before
    # Dash's own walk reaches map_shell_concept.py's turn, which then
    # unconditionally exec's it a second time, registering all ~50
    # callbacks twice. Deferring the import to inside layout() (only ever
    # called per-request, long after the whole pages/ walk has finished)
    # means sys.modules already has the real, once-executed module by the
    # time this import runs, so Python's normal import machinery just
    # reuses it: zero extra execution.
    import pages.map_shell_concept as ms

    # Sets the SAME module-level _LANG map_shell_concept.py itself uses,
    # a single-process global, not request-scoped (see that file's own note
    # on this), fine for this single-user concept page.
    ms._LANG = lang if lang in ms._TRANSLATIONS else "en"
    countries = [unquote(c) for c in zoom_countries.split(",") if c] if zoom_countries else []
    date = date or ms._DEFAULT_FORECAST_DATE
    run = run or ms._DEFAULT_FORECAST_RUN
    # Query params arrive as "1"/"0" strings (see _breakdown_new_tab_href).
    # None (no param at all, e.g. a bookmarked/hand-typed URL) falls back to
    # the sidebar's own checkbox defaults (all four hazards on) rather than
    # assuming everything's off.
    wind_on = (wind != "0") if wind is not None else True
    river_on = (river != "0") if river is not None else True
    rain_on = (rain != "0") if rain is not None else True
    # Storm Surge has no real backend anywhere in this app, ever (see
    # ms-surge-on's own comment in map_shell_concept.py: permanently
    # checked=False, disabled=True in the live sidebar, not just sometimes-
    # unavailable like River/Rain's own per-country checks). The live UI
    # already makes it impossible to ever toggle on; this print page must
    # match that exactly rather than accept a `surge` URL param at all
    # (unlike wind/river/rain above, which DO have real data and so
    # legitimately follow whatever the live modal's own checkbox state
    # was when the "Open in new tab" link was built). Ignoring the query
    # param entirely (not just its "no param at all" default) closes the
    # one remaining way a 3-member Flood selection (River+Rain+Storm
    # Surge) could ever be reached (a hand-crafted `?surge=1` URL) since
    # that combination only ever had a real per-hazard split for River+
    # Rain, never a real one to fall back to.
    surge_on = False
    # Slider indices (for the "Threshold sensitivity" preview curves).
    # Query params arrive as strings; None (no param, e.g. a hand-typed URL)
    # means that hazard's curve is simply skipped (see
    # _hazard_threshold_preview), not defaulted to some guessed position.
    wind_idx = int(windidx) if windidx is not None else None
    river_idx = int(riveridx) if riveridx is not None else None
    rain_idx = int(rainidx) if rainidx is not None else None
    surge_idx = int(surgeidx) if surgeidx is not None else None
    # River's own window is a real int (24/72/120/168, used directly as a
    # STEP_H SQL bind param downstream), unlike rain_window which stays a
    # string throughout (it's a _RAIN_MM_BY_WINDOW dict KEY, not a bind
    # param), cast explicitly rather than passing the raw URL string through.
    river_window = int(riverwindow) if riverwindow is not None else None
    # Rainfall's own accumulation window ("6"/"24"/"72"/"120"): which of the
    # 4 lines in its multi-line curve is "current" (see _rain_threshold_chart).

    # Resolves the storm badge against this page's own date/run (already
    # parsed above, shown in the "Forecast issued" line, and threaded into
    # _impact_breakdown_content below) rather than ms._STORMS, which is a
    # frozen "active right now" snapshot and would show the wrong storm's
    # badge (or none) for a historical or non-currently-active forecast
    # date. ms._resolve_storm_for_country is the same date-reactive lookup
    # the live modal itself uses: try each selected country in turn, first
    # match wins.
    storm = next(filter(None, (ms._resolve_storm_for_country(c, date, run) for c in countries)), None)

    info_items = []
    if storm:
        info_items.append(html.Div([
            html.Span(ms._t("Storm:"), style={"color": "#8ea0ab", "marginRight": "6px"}),
            html.Span(storm["name"], style={"fontWeight": 700}),
            html.Span(ms._cat_badge(storm["cat"]), style={"marginLeft": "8px", "verticalAlign": "middle"}),
        ]))
    info_items.append(html.Div([
        html.Span(ms._t("Forecast issued:"), style={"color": "#8ea0ab", "marginRight": "6px"}),
        html.Span(f"{date} · {run}Z", style={"fontWeight": 700}),
    ]))
    info_items.append(html.Div([
        html.Span(ms._t("Countries:"), style={"color": "#8ea0ab", "marginRight": "6px"}),
        html.Span(", ".join(ms._t(c) for c in countries) if countries else ms._t("Global"), style={"fontWeight": 700}),
    ]))
    # Wall-clock, not forecast-related, just when this report was rendered,
    # same idea as an email's own "sent at" timestamp.
    info_items.append(html.Div([
        html.Span(ms._t("Report generated:"), style={"color": "#8ea0ab", "marginRight": "6px"}),
        html.Span(datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M UTC"), style={"fontWeight": 700}),
    ]))

    return html.Div([
        html.Div([
            html.H1(ms._t("Full Impact Breakdown"), style={"fontSize": "20px", "fontWeight": 700,
                                                              "color": "#16232c", "margin": "0 0 12px"}),
            html.Div(info_items, style={"display": "flex", "flexWrap": "wrap", "gap": "10px 28px",
                                          "fontSize": "12.5px", "color": "#16232c"}),
        ], style={"marginBottom": "24px", "paddingBottom": "16px", "borderBottom": "1px solid #eef2f5"}),
        # expand_admin1=True: this static report has no click-to-expand
        # interaction a reader could use, so the Admin Level 1 sections
        # (collapsed by default in the interactive modal) render already
        # open here.
        # date/run (this page's own parsed query params, shown above in the
        # "Forecast issued" line) thread through to _impact_breakdown_content
        # too, so the table resolves against the same forecast date/run as
        # the page header rather than whatever storm happens to be active
        # right now.
        ms._impact_breakdown_content(countries, None, expand_admin1=True,
                                       wind_on=wind_on, river_on=river_on, rain_on=rain_on, surge_on=surge_on,
                                       wind_idx=wind_idx, river_idx=river_idx, rain_idx=rain_idx, surge_idx=surge_idx,
                                       rain_window=rainwindow, river_window=river_window, date=date, run=run),
    ], style={"background": "#ffffff", "maxWidth": "1400px", "margin": "0 auto", "padding": "32px",
               "fontFamily": "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif", "color": "#16232c"})

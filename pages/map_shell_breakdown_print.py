"""
Standalone, print-friendly Full Impact Breakdown — companion to the main
dashboard's (pages/map_shell_concept.py) own Full Impact Breakdown modal.
That modal's "open in new tab" link points here instead of just reopening
the dashboard with the modal auto-opened on top of the full app chrome
(topbar/map/side panels), which isn't something you'd ever want to print or
hand someone as a link — this is just the report itself: a plain white page
with the forecast context (storm, forecast run, countries) up top, the
table, and the Admin Level 1 breakdowns below.

No dedicated Print button — the browser's own print (Cmd/Ctrl+P) already
covers that, and the page's @page CSS (assets/map_shell_breakdown_print.css)
already gives it sane margins.

Not linked from the header nav — reachable only via the modal's own link
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
    # unconditionally exec_module()s every file with "register_page" in it —
    # it never checks sys.modules first, so it always executes
    # map_shell_concept.py exactly once during that walk regardless of
    # anything this file does. The bug was a top-level `import
    # pages.map_shell_concept` in THIS file: because "map_shell_breakdown_
    # print.py" sorts alphabetically before "map_shell_concept.py", Dash's
    # walk reached this file first, and a module-level import here ran
    # Python's normal import machinery (a genuine first execution,
    # registering every @callback for real) before Dash's walk ever got to
    # map_shell_concept.py's own turn — which then unconditionally exec'd it
    # a SECOND time, registering all ~50 callbacks twice. Deferring the
    # import to inside layout() (only ever called per-request, long after
    # the whole pages/ walk has finished) means sys.modules already has the
    # real, once-executed module by the time this import runs, so Python's
    # normal import machinery just reuses it — zero extra execution.
    import pages.map_shell_concept as ms

    # Sets the SAME module-level _LANG map_shell_concept.py itself uses —
    # single-process global, not request-scoped (see that file's own note
    # on this), fine for this single-user concept page.
    ms._LANG = lang if lang in ms._TRANSLATIONS else "en"
    countries = [unquote(c) for c in zoom_countries.split(",") if c] if zoom_countries else []
    date = date or ms._DEFAULT_FORECAST_DATE
    run = run or ms._DEFAULT_FORECAST_RUN
    # Query params arrive as "1"/"0" strings (see _breakdown_new_tab_href) —
    # None (no param at all, e.g. a bookmarked/hand-typed URL) falls back to
    # the sidebar's own checkbox defaults (all four hazards on) rather than
    # assuming everything's off.
    wind_on = (wind != "0") if wind is not None else True
    river_on = (river != "0") if river is not None else True
    rain_on = (rain != "0") if rain is not None else True
    surge_on = (surge != "0") if surge is not None else True
    # Slider indices (for the "Threshold sensitivity" preview curves) —
    # query params arrive as strings; None (no param, e.g. a hand-typed URL)
    # means that hazard's curve is simply skipped (see
    # _hazard_threshold_preview), not defaulted to some guessed position.
    wind_idx = int(windidx) if windidx is not None else None
    river_idx = int(riveridx) if riveridx is not None else None
    rain_idx = int(rainidx) if rainidx is not None else None
    surge_idx = int(surgeidx) if surgeidx is not None else None
    # River's own window is a real int (24/72/120/168 — used directly as a
    # STEP_H SQL bind param downstream), unlike rain_window which stays a
    # string throughout (it's a _RAIN_MM_BY_WINDOW dict KEY, not a bind
    # param) — cast explicitly rather than passing the raw URL string through.
    river_window = int(riverwindow) if riverwindow is not None else None
    # Rainfall's own accumulation window ("6"/"24"/"72"/"120") — which of the
    # 4 lines in its multi-line curve is "current" (see _rain_threshold_chart).

    # Real bug found+fixed here (2026-08, multi-agent hardcoded-mock-data
    # sweep): this used to match against ms._STORMS, a frozen "active right
    # now" snapshot, completely ignoring this page's own date/run (already
    # parsed above, shown in the "Forecast issued" line, and correctly
    # threaded into _impact_breakdown_content below) — a report for a
    # historical or non-"currently active" forecast date could show the
    # wrong storm's badge, or none at all even though real data existed.
    # ms._resolve_storm_for_country is the same real, date-reactive lookup
    # the live modal itself uses — try each selected country in turn (first
    # real match wins) rather than re-deriving from the static snapshot.
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
    # Wall-clock, not forecast-related — just when this report was rendered,
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
        # expand_admin1=True — this static report has no click-to-expand
        # interaction a reader could use, so the Admin Level 1 sections
        # (collapsed by default in the interactive modal) render already
        # open here.
        # date/run (this page's own already-correctly-parsed query params,
        # shown above in the "Forecast issued" line) now thread through to
        # _impact_breakdown_content too — previously they had nowhere to go
        # (that function didn't accept them yet), so this table silently
        # fell back to resolving against whatever storm happened to be
        # "active right now" instead of the date/run this page's own header
        # was already, correctly, displaying.
        ms._impact_breakdown_content(countries, None, expand_admin1=True,
                                       wind_on=wind_on, river_on=river_on, rain_on=rain_on, surge_on=surge_on,
                                       wind_idx=wind_idx, river_idx=river_idx, rain_idx=rain_idx, surge_idx=surge_idx,
                                       rain_window=rainwindow, river_window=river_window, date=date, run=run),
    ], style={"background": "#ffffff", "maxWidth": "1400px", "margin": "0 auto", "padding": "32px",
               "fontFamily": "-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif", "color": "#16232c"})

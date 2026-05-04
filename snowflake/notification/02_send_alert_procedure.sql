-- ==============================================================================
-- 07b_alert_agent/02_send_alert_procedure.sql — SEND_NEW_STORM_ALERT()
-- Deploy via Python connector (snow sql cannot handle { chars in f-strings).
-- Deploy 01_map_udf.sql first — the procedure calls GENERATE_ADMIN_MAP_PNG().
-- ==============================================================================

USE ROLE SYSADMIN;
USE DATABASE AOTS;
USE SCHEMA TC_ECMWF;
USE WAREHOUSE AOTS_WH;

CREATE OR REPLACE PROCEDURE SEND_NEW_STORM_ALERT()
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'main'
    EXECUTE AS OWNER
AS
$$
import json
import math
import re

def main(session):

    # ─── Helpers ─────────────────────────────────────────────────────────────

    def run(sql):
        """Execute a fire-and-forget SQL statement (result discarded)."""
        session.sql(sql).collect()

    def call_proc(sql):
        """Call a stored procedure and return its parsed JSON result, or None on failure."""
        rows = session.sql(sql).collect()
        if not rows:
            return None
        v = rows[0][0]
        if v is None:
            return None
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return None
        return v

    def scalar(sql):
        """Execute SQL and return the first cell as a string, or None."""
        rows = session.sql(sql).collect()
        return rows[0][0] if rows else None

    def q(s):
        """Escape single quotes in a value for safe SQL literal embedding."""
        return str(s or '').replace("'", "''")

    def fmt_n(n):
        """Format a number with thousands separators; returns 'N/A' for None."""
        if n is None:
            return 'N/A'
        return f'{math.ceil(n):,}'

    def fmt_delta(current, previous):
        """Return a colored HTML delta badge (▲/▼) comparing current to previous value."""
        if previous is None:
            return ''
        d = round(current - previous)
        if d == 0:
            return ''
        arrow = '▲' if d > 0 else '▼'
        color = '#c0392b' if d > 0 else '#27ae60'
        return f' <span style="font-size:0.8em; color:{color}; white-space:nowrap;">{arrow} {fmt_n(abs(d))}</span>'

    def pct(part, total):
        """Return (part/total)*100 formatted to one decimal place."""
        if not total or total == 0:
            return '0.0'
        return f'{(part / total) * 100:.1f}'

    def lbl(label_type):
        """Return an HTML provenance label badge for 'data' or 'inferred'."""
        if label_type == 'inferred':
            s = 'background:#fefce8; border:1px solid #e5d47a; color:#8a6e18;'
        else:
            s = 'background:#f0f8fc; border:1px solid #9ecde8; color:#1a6080;'
        return (f'<code style="{s} border-radius:2px; padding:0 3px; font-size:0.73em; '
                f'margin-left:3px; white-space:nowrap; vertical-align:middle;">{label_type}</code>')

    def fmt_time(utc_str, local_str, tz_offset):
        """Return local time with tz offset, or UTC string as fallback."""
        if local_str:
            return local_str + (f' ({tz_offset})' if tz_offset else '')
        return utc_str + ' UTC'

    def parse_sections(text):
        """Split a ===KEY=== delimited LLM response into a dict of section texts.
        Tolerates spacing and case variations around the markers."""
        import re
        keys = ['SUMMARY', 'NARRATIVE', 'SHIFT']
        result = {k: '' for k in keys}
        # Normalise any === WORD === variant to ===WORD===
        normalised = re.sub(r'===\s*(\w+)\s*===', lambda m: f'==={m.group(1).upper()}===', text)
        for i, key in enumerate(keys):
            marker = f'==={key}==='
            start = normalised.find(marker)
            if start == -1:
                continue
            start += len(marker)
            end = len(normalised)
            for other in keys[i + 1:]:
                pos = normalised.find(f'==={other}===', start)
                if pos != -1 and pos < end:
                    end = pos
            result[key] = normalised[start:end].strip()
        return result

    # ─── Static lookup dicts ──────────────────────────────────────────────────

    threshold_labels = {
        34:  'Storm Force (34kt)',
        40:  'Storm Force (40kt)',
        50:  'Strong Storm Force (50kt)',
        64:  'Category 1 Hurricane (64kt)',
        83:  'Category 2 Hurricane (83kt)',
        96:  'Category 3 Hurricane (96kt)',
        113: 'Category 4 Hurricane (113kt)',
        137: 'Category 5 Hurricane (137kt)'
    }

    ss_scale = {
        34:  {'category': 'Storm Force (34kt)',            'damage': 'storm-force conditions — downed branches, localized power outages'},
        40:  {'category': 'Storm Force (40kt)',            'damage': 'storm-force conditions — downed trees, power outages'},
        50:  {'category': 'Strong Storm Force (50kt)',     'damage': 'strong storm-force conditions — significant structural and tree damage'},
        64:  {'category': 'Category 1 Hurricane (64kt)',  'damage': 'very dangerous winds; roof, shingle and siding damage; power outages lasting days'},
        83:  {'category': 'Category 2 Hurricane (83kt)',  'damage': 'extremely dangerous winds; major roof and siding damage; near-total power loss lasting days to weeks'},
        96:  {'category': 'Category 3 Hurricane (96kt)',  'damage': 'devastating damage; roof decking removal; no electricity or water for days to weeks'},
        113: {'category': 'Category 4 Hurricane (113kt)', 'damage': 'catastrophic damage; severe structural loss; power outages lasting weeks to months'},
        137: {'category': 'Category 5 Hurricane (137kt)', 'damage': 'catastrophic damage; high percentage of homes destroyed; areas uninhabitable for weeks to months'}
    }


    # ─── Step 1: Find new (storm, country) pairs not yet alerted ─────────────

    new_pairs = []
    for row in session.sql("""
        SELECT DISTINCT
            te.TRACK_ID,
            TO_CHAR(te.FORECAST_TIME, 'YYYY-MM-DD HH24:MI:SS') AS FORECAST_TIME,
            pc.COUNTRY_CODE,
            pc.COUNTRY_NAME,
            COALESCE(pc.TIMEZONE, 'UTC') AS TIMEZONE
        FROM TC_ENVELOPES_COMBINED te
        JOIN PIPELINE_COUNTRIES pc
             ON ST_DWITHIN(pc.COUNTRY_BOUNDARY, te.ENVELOPE_REGION, 1500000)
        LEFT JOIN ALERT_SENT_LOG asl
             ON asl.TRACK_ID      = te.TRACK_ID
            AND asl.FORECAST_TIME = te.FORECAST_TIME
            AND asl.COUNTRY_CODE  = pc.COUNTRY_CODE
        WHERE pc.ACTIVE = TRUE
          AND te.FORECAST_TIME >= DATEADD('day', -3, CURRENT_TIMESTAMP())
          AND asl.TRACK_ID IS NULL
        ORDER BY FORECAST_TIME DESC, TRACK_ID
    """).collect():
        new_pairs.append({
            'track_id':     row[0],
            'forecast_time': row[1],
            'country_code':  row[2],
            'country_name':  row[3],
            'timezone':      row[4]
        })

    if not new_pairs:
        return 'OK: no new storm/country pairs to alert'

    alerts = []
    errors  = []

    for pair in new_pairs:
        t_id = q(pair['track_id'])
        t_ft = q(str(pair['forecast_time']))
        t_cc = q(pair['country_code'])

        try:

            # ── 2a: Derive forecast date + canonical storm name ───────────────
            forecast_date = re.sub(r'[-: ]', '', pair['forecast_time'])
            t_fd          = q(forecast_date)
            date_result   = call_proc(f"CALL GET_LATEST_FORECAST_DATE('{t_cc}', '{t_id}')")
            storm_name    = (date_result['latest_storm']
                             if date_result and date_result.get('latest_storm')
                             else pair['track_id'])
            t_sn = q(storm_name)

            # ── 2b: Expected impact at 50kt (full age breakdown) ─────────────
            exp = call_proc(f"CALL GET_EXPECTED_IMPACT_VALUES('{t_cc}', '{t_sn}', '{t_fd}', '50')")
            if not exp or not exp.get('total_population'):
                errors.append(f"{pair['track_id']}/{pair['country_code']}: no 50kt impact data")
                continue

            # ── 2c: All wind thresholds (cross-threshold table) ───────────────
            all_thresh = call_proc(f"CALL GET_ALL_WIND_THRESHOLDS_ANALYSIS('{t_cc}', '{t_sn}', '{t_fd}')")
            thresholds = all_thresh['thresholds'] if all_thresh and all_thresh.get('thresholds') else []

            # ── 2d: Admin breakdown at 50kt ───────────────────────────────────
            breakdown   = call_proc(f"CALL GET_ADMIN_LEVEL_BREAKDOWN('{t_cc}', '{t_sn}', '{t_fd}', '50')")
            admin_areas = breakdown['admin_areas'] if breakdown and breakdown.get('admin_areas') else []

            # ── 2e: Previous forecast comparison ─────────────────────────────
            prev_date       = None
            prev_pop        = None
            pop_delta       = None
            admin_delta_map = {}
            prev_result = call_proc(f"CALL GET_PREVIOUS_FORECAST_DATE('{t_cc}', '{t_sn}', '{t_fd}')")
            if prev_result and prev_result.get('has_previous') and prev_result.get('previous_forecast_date'):
                prev_date = prev_result['previous_forecast_date']
                prev_exp  = call_proc(f"CALL GET_EXPECTED_IMPACT_VALUES('{t_cc}', '{t_sn}', '{prev_date}', '50')")
                if prev_exp and prev_exp.get('total_population'):
                    prev_pop  = prev_exp['total_population']
                    pop_delta = exp['total_population'] - prev_pop
                admin_trend = call_proc(
                    f"CALL GET_ADMIN_LEVEL_TREND_COMPARISON('{t_cc}', '{t_sn}', '{t_fd}', '{prev_date}', '50')"
                )
                if admin_trend and admin_trend.get('admin_trends'):
                    for tr in admin_trend['admin_trends']:
                        admin_delta_map[tr['administrative_area']] = tr

            # ── 2f: Storm arrival timing ──────────────────────────────────────
            timing = None
            timing_result = call_proc(f"CALL GET_STORM_ARRIVAL_TIMING('{t_cc}', '{t_sn}', '{t_fd}', '50')")
            if timing_result and timing_result.get('has_timing'):
                timing = timing_result

            # ── 2f-tz: Convert timing values to local time ────────────────────
            if timing and timing.get('has_timing'):
                tz_name = pair['timezone']
                if tz_name:
                    try:
                        t0  = timing.get('earliest_impact_time')
                        t1  = timing.get('consensus_impact_time')
                        t2  = timing.get('latest_impact_time')
                        ref = t0 or t1 or t2
                        cols = [
                            f"TO_CHAR(CONVERT_TIMEZONE('UTC','{tz_name}','{t0}'::TIMESTAMP_NTZ),'Mon DD HH24:MI')" if t0 else 'NULL',
                            f"TO_CHAR(CONVERT_TIMEZONE('UTC','{tz_name}','{t1}'::TIMESTAMP_NTZ),'Mon DD HH24:MI')" if t1 else 'NULL',
                            f"TO_CHAR(CONVERT_TIMEZONE('UTC','{tz_name}','{t2}'::TIMESTAMP_NTZ),'Mon DD HH24:MI')" if t2 else 'NULL',
                            f"DATEDIFF('minute','{ref}'::TIMESTAMP_NTZ,"
                            f"CONVERT_TIMEZONE('UTC','{tz_name}','{ref}'::TIMESTAMP_NTZ)::TIMESTAMP_NTZ)"
                        ]
                        tz_rows = session.sql(f"SELECT {', '.join(cols)}").collect()
                        if tz_rows:
                            timing['earliest_local']  = tz_rows[0][0]
                            timing['consensus_local'] = tz_rows[0][1]
                            timing['latest_local']    = tz_rows[0][2]
                            offset_min = tz_rows[0][3]
                            sign   = '+' if offset_min >= 0 else '−'
                            abs_m  = abs(offset_min)
                            h, m   = math.floor(abs_m / 60), abs_m % 60
                            timing['tz_offset'] = f'UTC{sign}{h}' + (f':{str(m).zfill(2)}' if m > 0 else '')
                    except Exception as tz_err:
                        errors.append(f'TZ conversion failed: {tz_err}')

            # ── 2g: Centroid shift since previous forecast ────────────────────
            centroid_shift = None
            shift_result   = call_proc(f"CALL GET_CENTROID_SHIFT('{t_cc}', '{t_sn}', '{t_fd}', '50')")
            if shift_result and shift_result.get('has_previous') and (shift_result.get('dist_km') or 0) >= 5:
                centroid_shift = shift_result

            # ── 2h: Admin-level GeoJSON for PNG map ────────────────────────────
            map_geo_rows = []
            try:
                for geo_row in session.sql(f"""
                    SELECT
                        g.NAME                                                                AS admin_name,
                        CAST(ST_ASGEOJSON(g.GEOMETRY) AS VARCHAR)                             AS geojson,
                        ST_X(ST_CENTROID(g.GEOMETRY))                                         AS centroid_lon,
                        ST_Y(ST_CENTROID(g.GEOMETRY))                                         AS centroid_lat,
                        COALESCE(a.children_at_risk, 0)                                       AS children_at_risk
                    FROM AOTS.TC_ECMWF.BASE_ADMIN_GEOM_MAT g
                    LEFT JOIN (
                        SELECT NAME,
                               SUM(COALESCE(E_SCHOOL_AGE_POPULATION, 0) + COALESCE(E_INFANT_POPULATION, 0)
                                   + COALESCE(E_ADOLESCENT_POPULATION, 0)) AS children_at_risk
                        FROM AOTS.TC_ECMWF.ADMIN_ALL_IMPACT_MAT
                        WHERE COUNTRY = '{t_cc}'
                          AND UPPER(STORM) = UPPER('{t_sn}')
                          AND FORECAST_DATE = RPAD(REGEXP_REPLACE('{t_fd}', '[^0-9]', ''), 14, '0')
                          AND WIND_THRESHOLD = 50
                          AND ADMIN_LEVEL = 1
                        GROUP BY NAME
                    ) a ON g.NAME = a.NAME
                    WHERE g.COUNTRY = '{t_cc}'
                      AND g.ADMIN_LEVEL = 1
                    ORDER BY COALESCE(a.children_at_risk, 0) DESC
                """).collect():
                    map_geo_rows.append({
                        'name':     geo_row[0],
                        'geojson':  geo_row[1],
                        'clon':     geo_row[2],
                        'clat':     geo_row[3],
                        'children': geo_row[4] or 0
                    })
            except Exception as e:
                errors.append(f'Map geo query failed: {e}')

            # ── 3: Build context and call CORTEX.COMPLETE for all prose sections ─
            admin_summary = ', '.join(
                f"{a['administrative_area']} ({fmt_n(a['population'])} people; "
                f"{pct(a['population'], exp['total_population'])}% of national total)"
                for a in admin_areas[:3]
            )

            thresh_ctx_parts = []
            for th in thresholds:
                if (th.get('total_population') or 0) > 0:
                    tw     = th['wind_threshold']
                    t_lbl  = threshold_labels.get(tw, f'{tw}kt')
                    ss_inf = (f" [{ss_scale[tw]['category']}: {ss_scale[tw]['damage']}]"
                              if tw in ss_scale else '')
                    thresh_ctx_parts.append(f"{t_lbl}: {fmt_n(th['total_population'])} people{ss_inf}")
            thresh_ctx = '; '.join(thresh_ctx_parts)

            if pop_delta is not None:
                direction = 'INCREASING — up' if pop_delta > 0 else 'DECREASING — down'
                trend_str = (f"{direction} {fmt_n(abs(pop_delta))} from previous run "
                             f"({fmt_n(prev_pop)} → {fmt_n(exp['total_population'])} people at 50kt)")
            else:
                trend_str = 'No previous forecast available for comparison.'

            # Build centroid shift context for the LLM (SHIFT section)
            use_shift_llm = (
                centroid_shift and
                centroid_shift.get('has_previous') and
                (centroid_shift.get('dist_km') or 0) >= 5
            )
            shift_ctx = 'not available'
            if use_shift_llm:
                _dir_plain = {'N':'north','NE':'northeast','E':'east','SE':'southeast',
                              'S':'south','SW':'southwest','W':'west','NW':'northwest'}
                _dir = _dir_plain.get(centroid_shift.get('direction', ''), centroid_shift.get('direction', ''))
                shift_ctx = f"{centroid_shift['dist_km']} km toward the {_dir}"
                tg = centroid_shift.get('top_gainer')
                tl = centroid_shift.get('top_loser')
                if tg and tg.get('name'):
                    shift_ctx += f"; top gainer: {tg['name']} (+{fmt_n(tg['delta'])} children at risk)"
                if tl and tl.get('name') and abs(tl.get('delta', 0)) >= 200:
                    shift_ctx += f"; top loser: {tl['name']} ({fmt_n(tl['delta'])} children at risk)"

            narrative_context = (
                f"Storm: {storm_name} | Country: {pair['country_name']} | Forecast: {forecast_date}\n"
                f"Population at risk (50kt): {fmt_n(exp.get('total_population'))}\n"
                f"Children at risk (0-19): {fmt_n(exp.get('total_children'))}\n"
                f"  Age 0-4 (infants): {fmt_n(exp.get('total_infant_children'))}\n"
                f"  Age 5-14 (school-age): {fmt_n(exp.get('total_school_age_children'))}\n"
                f"  Age 15-19 (adolescents): {fmt_n(exp.get('total_adolescent_children'))}\n"
                f"Schools at risk (50kt): {fmt_n(exp.get('total_schools'))}\n"
                f"Health centers at risk (50kt): {fmt_n(exp.get('total_hcs'))}\n"
                + (f"Shelters at risk: {fmt_n(exp.get('total_shelters'))}\n"
                   if exp.get('total_shelters') is not None else '')
                + (f"WASH facilities at risk: {fmt_n(exp.get('total_wash'))}\n"
                   if exp.get('total_wash') is not None else '')
                + f"Population by wind speed: {thresh_ctx}\n"
                f"Most affected areas (50kt): {admin_summary or 'N/A'}\n"
                f"Forecast trend vs previous run: {trend_str}\n"
                f"Centroid shift: {shift_ctx}"
            )

            data_label = ('<code style="background:#f0f8fc; border:1px solid #9ecde8; border-radius:2px; '
                          'padding:0 3px; font-size:0.73em; color:#1a6080; margin-left:3px; '
                          'white-space:nowrap; vertical-align:middle;">data</code>')

            combined_prompt = (
                'Generate three sections of HTML text for a storm alert email sent to '
                'UNICEF emergency responders.\n\n'
                'Output EXACTLY this structure — no other text before, between, or after the markers:\n'
                '===SUMMARY===\n'
                '(your summary here)\n'
                '===NARRATIVE===\n'
                '(your narrative here)\n'
                '===SHIFT===\n'
                '(your shift text here, or leave blank if Centroid shift in DATA is "not available")\n\n'
                '--- SECTION REQUIREMENTS ---\n\n'
                '===SUMMARY=== (1–2 sentences maximum)\n'
                'The headline read — for someone who may only read this one line.\n'
                'Cover: storm name, country, 50kt exposure (people + children), top region at risk.\n'
                'Make it urgent and human — this is the first thing a responder sees.\n'
                'Do NOT mention trend, timing, or higher wind thresholds here.\n\n'
                '===NARRATIVE=== (4–6 sentences)\n'
                'MUST include:\n'
                '- 50kt exposure with which regions carry most risk\n'
                '- Child-focused lens: children at risk, schools and health centers as '
                'vulnerability indicators, frame higher thresholds in terms of what they '
                'mean for children and families\n'
                '- EVERY wind threshold in DATA with population > 0. For 64kt+: state '
                'population AND damage consequences (from DATA). Do not omit these.\n'
                '- Whether overall exposure is growing or shrinking vs the previous forecast\n'
                'TONE: Brief an emergency response manager — explain consequences, not just '
                'numbers. The headline totals are already shown above; your value is the '
                'risk profile and implications.\n\n'
                '===SHIFT=== (2–3 sentences, or blank if Centroid shift is "not available")\n'
                'Explain what the shift in forecast impact means — not just the numbers, '
                'but what it tells us about how the risk picture is evolving.\n'
                'MUST include:\n'
                '- Direction and distance of the shift\n'
                '- Which areas are gaining exposure and which are seeing reduced exposure\n'
                '  (use DATA for names and child deltas)\n'
                '- Context: is the top-gaining area already the highest-risk area? If so, '
                'note that concentration of risk is increasing. Are areas seeing their first '
                'meaningful exposure? Note that too.\n'
                'LANGUAGE RULES:\n'
                '- A decrease in expected exposure is a positive development — phrase it as '
                '"X fewer children at risk" or "reduced exposure", not "loses children"\n'
                '- WRONG: "The storm has shifted" or "The storm track moved N km"\n'
                '- RIGHT: "The latest forecast shows the expected impact footprint shifting '
                'roughly N km westward"\n'
                '- Do NOT make operational recommendations (do not suggest where to send '
                'resources, where to focus response, or what responders should do)\n'
                'This is a change in the probability distribution of impact, not a track move.\n\n'
                '--- RULES FOR ALL THREE SECTIONS ---\n'
                '- Valid HTML sentences only — no markdown, no bullet points\n'
                '- Never invent any number not present in DATA\n'
                '- Never use: "ensemble", "probabilistic", "members", "spread", "percentile"\n'
                '- Use conditional language for impacts: "could", "may", "risk of", "potential for"\n'
                f'- After EVERY number cited, insert this HTML immediately after it '
                f'(no space before): {data_label}\n'
                f'  Example: "Approximately 1,732,559{data_label} people may face '
                f'storm-force winds."\n\n'
                f'DATA:\n{narrative_context}'
            )

            safe_prompt = combined_prompt.replace("'", "''")
            system_msg  = ('You write factual storm alert email content for UNICEF emergency '
                           'responders. Use only the data provided. Never invent numbers. '
                           'Always respond in the exact ===SECTION=== format specified.')
            llm_raw = scalar(
                f"SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-4-sonnet', '{system_msg}\n\n{safe_prompt}')"
            ) or ''
            llm            = parse_sections(llm_raw)
            situation_overview = llm.get('NARRATIVE', '')
            summary_text       = llm.get('SUMMARY', '')
            shift_llm_text     = llm.get('SHIFT', '').strip()

            # ── 4: Build HTML email ───────────────────────────────────────────

            fd      = str(forecast_date)
            months  = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
            fd_year = fd[0:4]
            fd_month = months[int(fd[4:6]) - 1]
            fd_day   = int(fd[6:8])
            fd_hour  = fd[8:10]
            fd_formatted      = f'{fd_month} {fd_day}, {fd_year} {fd_hour}Z UTC'
            fd_formatted_full = fd_formatted

            tz_name2 = pair['timezone']
            if tz_name2:
                try:
                    fd_utc = f"{fd_year}-{fd[4:6]}-{fd[6:8]} {fd_hour}:00"
                    fd_tz  = session.sql(
                        f"SELECT TO_CHAR(CONVERT_TIMEZONE('UTC','{tz_name2}','{fd_utc}'::TIMESTAMP_NTZ),'Mon DD, YYYY HH24:MI'),"
                        f"DATEDIFF('minute','{fd_utc}'::TIMESTAMP_NTZ,"
                        f"CONVERT_TIMEZONE('UTC','{tz_name2}','{fd_utc}'::TIMESTAMP_NTZ)::TIMESTAMP_NTZ)"
                    ).collect()
                    if fd_tz:
                        fd_local   = fd_tz[0][0]
                        fd_off_min = fd_tz[0][1]
                        fd_sign    = '+' if fd_off_min >= 0 else '−'
                        fd_h, fd_m = math.floor(abs(fd_off_min) / 60), abs(fd_off_min) % 60
                        fd_offset  = f'UTC{fd_sign}{fd_h}' + (f':{str(fd_m).zfill(2)}' if fd_m > 0 else '')
                        fd_formatted_full = f'{fd_formatted} / {fd_local} local ({fd_offset})'
                except Exception:
                    pass

            # Impact bullet list
            impact_bullets = (
                '<ul style="margin:8px 0; padding-left:20px;">'
                f'<li>Expected population at risk: <strong>{fmt_n(exp.get("total_population"))}</strong>{lbl("data")}</li>'
                f'<li>Expected children at risk (0–19): <strong>{fmt_n(exp.get("total_children"))}</strong>{lbl("data")}'
                '<ul style="margin:4px 0; padding-left:20px;">'
                f'<li>Age 0–4 (infants): {fmt_n(exp.get("total_infant_children"))}{lbl("data")}</li>'
                f'<li>Age 5–14 (school-age): {fmt_n(exp.get("total_school_age_children"))}{lbl("data")}</li>'
                f'<li>Age 15–19 (adolescents): {fmt_n(exp.get("total_adolescent_children"))}{lbl("data")}</li>'
                '</ul></li>'
                f'<li>Expected schools at risk: <strong>{fmt_n(exp.get("total_schools"))}</strong>{lbl("data")}</li>'
                f'<li>Expected health centers at risk: <strong>{fmt_n(exp.get("total_hcs"))}</strong>{lbl("data")}</li>'
                + (f'<li>Expected shelters at risk: <strong>{fmt_n(exp.get("total_shelters"))}</strong>{lbl("data")}</li>'
                   if exp.get('total_shelters') is not None else '')
                + (f'<li>Expected WASH facilities at risk: <strong>{fmt_n(exp.get("total_wash"))}</strong>{lbl("data")}</li>'
                   if exp.get('total_wash') is not None else '')
                + '</ul>'
            )

            # Admin breakdown table
            prev_date_fmt = (
                f'{months[int(prev_date[4:6]) - 1]} {int(prev_date[6:8])}, '
                f'{prev_date[0:4]} {prev_date[8:10]}Z'
                if prev_date else None
            )

            admin_table = ''
            if admin_areas:
                admin_has_shelters = any(a.get('shelters') is not None for a in admin_areas)
                admin_has_wash     = any(a.get('wash_facilities') is not None for a in admin_areas)
                s_th = ('<th style="text-align:right; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">Shelters</th>'
                        if admin_has_shelters else '')
                w_th = ('<th style="text-align:right; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">WASH</th>'
                        if admin_has_wash else '')
                admin_table = (
                    '<table style="border-collapse:collapse; width:100%; font-size:0.92em; margin-top:12px;">'
                    '<thead><tr style="background:#1CABE2;">'
                    '<th style="text-align:left; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">Admin Area</th>'
                    '<th style="text-align:right; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">Population</th>'
                    '<th style="text-align:right; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">Children (0–19)</th>'
                    '<th style="text-align:right; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">Schools</th>'
                    '<th style="text-align:right; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">Health Centers</th>'
                    f'{s_th}{w_th}'
                    '</tr></thead><tbody>'
                )
                for a_idx, area in enumerate(admin_areas):
                    row_bg = '#fff' if a_idx % 2 == 0 else '#f9f9f9'
                    d      = admin_delta_map.get(area['administrative_area'])
                    s_td   = (f'<td style="text-align:right; padding:6px 8px; border:1px solid #ddd;">'
                              f'{fmt_n(area.get("shelters", 0))}'
                              f'{fmt_delta(d["current_shelters"], d["previous_shelters"]) if d else ""}</td>'
                              if admin_has_shelters else '')
                    w_td   = (f'<td style="text-align:right; padding:6px 8px; border:1px solid #ddd;">'
                              f'{fmt_n(area.get("wash_facilities", 0))}'
                              f'{fmt_delta(d["current_wash"], d["previous_wash"]) if d else ""}</td>'
                              if admin_has_wash else '')
                    admin_table += (
                        f'<tr style="background:{row_bg};">'
                        f'<td style="padding:6px 8px; border:1px solid #ddd;"><strong>{area["administrative_area"]}</strong></td>'
                        f'<td style="text-align:right; padding:6px 8px; border:1px solid #ddd;">'
                        f'{fmt_n(area.get("population"))}{fmt_delta(d["current_population"], d["previous_population"]) if d else ""}</td>'
                        f'<td style="text-align:right; padding:6px 8px; border:1px solid #ddd;">'
                        f'{fmt_n(area.get("children"))}{fmt_delta(d["current_children"], d["previous_children"]) if d else ""}</td>'
                        f'<td style="text-align:right; padding:6px 8px; border:1px solid #ddd;">'
                        f'{fmt_n(area.get("schools"))}{fmt_delta(d["current_schools"], d["previous_schools"]) if d else ""}</td>'
                        f'<td style="text-align:right; padding:6px 8px; border:1px solid #ddd;">'
                        f'{fmt_n(area.get("health_centers"))}{fmt_delta(d["current_hcs"], d["previous_hcs"]) if d else ""}</td>'
                        f'{s_td}{w_td}'
                        '</tr>'
                    )
                admin_table += (
                    '</tbody></table>'
                    f'<p style="font-size:0.88em; color:#777; margin-top:4px;">Expected impact at storm-force winds (50kt) '
                    f'by administrative area. {lbl("data")} Values are rounded to the nearest integer; '
                    'the sum across administrative areas may exceed the totals shown above.'
                    + (f' Trend arrows (▲/▼) compare to the previous forecast ({prev_date_fmt}).'
                       if prev_date_fmt and admin_delta_map else '')
                    + '</p>'
                )

            # Forecast stability / centroid shift section — always shown
            if use_shift_llm and shift_llm_text:
                # Significant shift (≥5 km) — LLM-generated analysis
                shift_section = (
                    f"<p style=\"margin:14px 0 4px;\">{shift_llm_text}</p>"
                    "<p style=\"font-size:0.88em; color:#999; margin:0 0 4px;\">" + lbl("data") +
                    " Shift computed from children-at-risk-weighted centroid of expected impact values "
                    "across all ECMWF forecast members (50kt). This reflects a change in the forecast's "
                    "overall probability distribution, not movement of a single storm track.</p>"
                )
            elif prev_date:
                # No significant shift — static stability note with overall population trend
                if pop_delta is not None and abs(pop_delta) >= 1000:
                    trend_dir  = 'increased' if pop_delta > 0 else 'decreased'
                    trend_note = (f' Overall, population at risk has {trend_dir} by '
                                  f'{fmt_n(abs(math.ceil(pop_delta)))} since the previous run '
                                  f'({fmt_n(prev_pop)} → {fmt_n(exp["total_population"])}).')
                else:
                    trend_note = ' Overall population at risk remains broadly similar to the previous run.'
                shift_section = (
                    f'<p style="margin:14px 0 4px; color:#555;">The expected impact footprint is '
                    f'<strong>broadly stable</strong> since the previous forecast ({prev_date_fmt}).'
                    f'{trend_note}</p>'
                    '<p style="font-size:0.88em; color:#999; margin:0 0 4px;">No significant geographic '
                    'shift detected in the ensemble-average impact distribution (shift &lt;&nbsp;5&nbsp;km). '
                    + lbl("data") + '</p>'
                )
            else:
                # No previous forecast available
                shift_section = (
                    '<p style="margin:14px 0 4px; color:#555;">'
                    'No previous forecast is available for comparison — '
                    'this is the first alert for this storm.</p>'
                )

            # Bottom-line box
            bl_pop      = fmt_n(round(exp.get('total_population', 0) / 1000) * 1000)
            bl_children = fmt_n(round(exp.get('total_children', 0)   / 1000) * 1000)
            bl_delta = ''
            if pop_delta is not None:
                bl_abs = fmt_n(abs(math.ceil(pop_delta)))
                if pop_delta > 1000:
                    bl_delta = f' <span style="color:#c0392b; font-weight:bold;">▲ +{bl_abs} people at risk since previous forecast.</span>'
                elif pop_delta < -1000:
                    bl_delta = f' <span style="color:#27ae60; font-weight:bold;">▼ −{bl_abs} people at risk since previous forecast.</span>'

            bl_timing = ''
            if timing and timing.get('has_timing') and timing.get('earliest_impact_hours') is not None:
                tz_sfx     = (' local, ' + timing['tz_offset']) if timing.get('tz_offset') else ' UTC'
                early_part = (f"~{timing['earliest_impact_hours']} hours "
                              f"({fmt_time(timing['earliest_impact_time'], timing.get('earliest_local'), None)}{tz_sfx})")
                if timing.get('consensus_impact_hours') is not None:
                    con_sfx      = (' local, ' + timing['tz_offset']) if timing.get('tz_offset') else ' UTC'
                    consensus_part = (f"~{timing['consensus_impact_hours']} hours "
                                      f"({fmt_time(timing['consensus_impact_time'], timing.get('consensus_local'), None)}{con_sfx})")
                    bl_timing = (f" Storm-force (50kt) winds could arrive as early as "
                                 f"<strong>{early_part}</strong>, most likely by <strong>{consensus_part}</strong>.")
                else:
                    bl_timing = f" Storm-force (50kt) winds could arrive as early as <strong>{early_part}</strong>."

            bottom_line = (
                '<div style="background:#f0f9ff; border-left:5px solid #1CABE2; padding:14px 18px; margin-bottom:20px; border-radius:0 3px 3px 0;">'
                '<div style="font-size:0.8em; color:#1A6080; font-weight:bold; text-transform:uppercase; letter-spacing:1px; margin-bottom:7px;">Situation Summary</div>'
                f'{summary_text or ""}{bl_delta}{bl_timing}'
                '</div>'
            )

            # PNG map
            map_html = None
            if map_geo_rows:
                try:
                    map_json = json.dumps(map_geo_rows)
                    png_rows = session.sql("SELECT AOTS.TC_ECMWF.GENERATE_ADMIN_MAP_PNG(?)", params=[map_json]).collect()
                    if png_rows:
                        png_val = png_rows[0][0]
                        if png_val and len(str(png_val)) > 100:
                            map_html = (f'<img src="data:image/png;base64,{png_val}" '
                                        'style="width:100%; max-width:700px; display:block; margin:0 auto;" '
                                        'alt="Admin impact map" />')
                except Exception as e:
                    errors.append(f'PNG map UDF failed: {e}')

            map_section = ''
            if map_html:
                map_section = (
                    '<p style="font-size:0.88em; font-weight:bold; color:#444; margin:18px 0 6px;">'
                    'Expected Children at Risk (0–19) by Admin Area (50kt)</p>'
                    + map_html +
                    '<p style="font-size:0.83em; color:#aaa; margin-top:4px; font-style:italic;">'
                    'The boundaries and names shown and the designations used on this map do not imply '
                    'official endorsement or acceptance by the United Nations.</p>'
                    '<p style="font-size:0.88em; color:#888; margin-top:2px;">Expected children (age 0–19) '
                    'at risk of storm-force wind exposure (50kt) by administrative area. '
                    f'{lbl("data")} Darker red = higher expected exposure. Values rounded to nearest integer.</p>'
                )

            # Timing box
            timing_box = ''
            if timing and timing.get('has_timing'):
                members_info = ''
                if timing.get('members_hitting') is not None and timing.get('total_members') is not None:
                    members_info = (f' &nbsp;&middot;&nbsp; <span style="font-weight:normal;">'
                                    f'{timing["members_hitting"]}/{timing["total_members"]} forecast members reach this country</span>')
                e_div = (f'<div style="margin:3px 0;"><span style="color:#27ae60; font-weight:bold;">Earliest:</span> '
                         f'~{timing["earliest_impact_hours"]} hours &mdash; '
                         f'{fmt_time(timing["earliest_impact_time"], timing.get("earliest_local"), timing.get("tz_offset"))}</div>'
                         if timing.get('earliest_impact_hours') is not None else '')
                c_div = (f'<div style="margin:3px 0;"><span style="color:#1CABE2; font-weight:bold;">Consensus (median):</span> '
                         f'~<strong>{timing["consensus_impact_hours"]} hours &mdash; '
                         f'{fmt_time(timing["consensus_impact_time"], timing.get("consensus_local"), timing.get("tz_offset"))}</strong></div>'
                         if timing.get('consensus_impact_hours') is not None else '')
                l_div = (f'<div style="margin:3px 0;"><span style="color:#c0392b; font-weight:bold;">Latest:</span> '
                         f'~{timing["latest_impact_hours"]} hours &mdash; '
                         f'{fmt_time(timing["latest_impact_time"], timing.get("latest_local"), timing.get("tz_offset"))}</div>'
                         if timing.get('latest_impact_hours') is not None else '')
                timing_box = (
                    '<div style="background:#f0f9ff; border:1px solid #b3e0f5; border-radius:3px; padding:12px 16px; margin-top:14px; font-size:0.92em;">'
                    f'<div style="font-size:0.8em; color:#1A6080; font-weight:bold; text-transform:uppercase; letter-spacing:1px; margin-bottom:6px;">'
                    f'Expected Storm-Force Wind Arrival (50kt){members_info}</div>'
                    f'{e_div}{c_div}{l_div}'
                    '</div>'
                )

            # Cross-threshold table
            has_shelters = any(th.get('total_shelters') is not None for th in thresholds)
            has_wash     = any(th.get('total_wash') is not None for th in thresholds)
            s_th2 = ('<th style="text-align:right; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">Shelters</th>'
                     if has_shelters else '')
            w_th2 = ('<th style="text-align:right; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">WASH Facilities</th>'
                     if has_wash else '')
            thresh_table = (
                '<table style="border-collapse:collapse; width:100%; font-size:0.92em;">'
                '<thead><tr style="background:#1CABE2;">'
                '<th style="text-align:left; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">Wind Speed</th>'
                '<th style="text-align:right; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">Population</th>'
                '<th style="text-align:right; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">Children (0–19)</th>'
                '<th style="text-align:right; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">Schools</th>'
                '<th style="text-align:right; padding:8px 10px; border:1px solid #1499c7; color:white; font-weight:bold;">Health Centers</th>'
                f'{s_th2}{w_th2}'
                '</tr></thead><tbody>'
            )
            for th in thresholds:
                if not th.get('total_population') or th['total_population'] == 0:
                    continue
                is_hl      = th['wind_threshold'] == 50
                row_style  = 'background:#ebf8ff; font-weight:bold; border-left:3px solid #1CABE2;' if is_hl else 'background:#fff;'
                t_lbl2     = threshold_labels.get(th['wind_threshold'], f"{th['wind_threshold']}kt")
                s_td2      = (f'<td style="text-align:right; padding:6px 8px; border:1px solid #ddd;">{fmt_n(th.get("total_shelters", 0))}</td>'
                              if has_shelters else '')
                w_td2      = (f'<td style="text-align:right; padding:6px 8px; border:1px solid #ddd;">{fmt_n(th.get("total_wash", 0))}</td>'
                              if has_wash else '')
                thresh_table += (
                    f'<tr style="{row_style}">'
                    f'<td style="padding:6px 8px; border:1px solid #ddd;">{t_lbl2}</td>'
                    f'<td style="text-align:right; padding:6px 8px; border:1px solid #ddd;">{fmt_n(th.get("total_population"))}</td>'
                    f'<td style="text-align:right; padding:6px 8px; border:1px solid #ddd;">{fmt_n(th.get("total_children"))}</td>'
                    f'<td style="text-align:right; padding:6px 8px; border:1px solid #ddd;">{fmt_n(th.get("total_schools"))}</td>'
                    f'<td style="text-align:right; padding:6px 8px; border:1px solid #ddd;">{fmt_n(th.get("total_hcs"))}</td>'
                    f'{s_td2}{w_td2}'
                    '</tr>'
                )
            thresh_table += '</tbody></table>'

            # Assemble full email
            email_body = (
                '<div style="font-family:Arial,sans-serif; max-width:700px; margin:0 auto; color:#222; border:1px solid #d0d0d0; line-height:1.6;">'

                '<div style="background:#1CABE2; padding:18px 24px;">'
                '<div style="color:white; font-size:0.78em; font-weight:bold; letter-spacing:1.5px; text-transform:uppercase; opacity:0.9;">Ahead of the Storm &nbsp;&mdash;&nbsp; Storm Alert</div>'
                '<div style="border-top:1px solid rgba(255,255,255,0.3); margin:12px 0 10px;"></div>'
                f'<div style="color:white; font-size:1.15em; font-weight:bold; letter-spacing:0.3px;">Storm {storm_name} &mdash; {pair["country_name"]}</div>'
                f'<div style="color:rgba(255,255,255,0.85); font-size:0.9em; margin-top:5px;">Forecast issued: {fd_formatted_full}</div>'
                '</div>'

                '<div style="padding:20px 24px;">'
                + bottom_line +
                '<div style="border-top:1px solid #e8e8e8; margin:18px 0;"></div>'
                f'<p style="margin:0 0 10px;">{situation_overview or ""}</p>'
                f'<p style="font-size:0.88em; color:#888; margin-top:0;">'
                f'{lbl("data")} = forecast data value &nbsp; {lbl("inferred")} = computed from data. '
                'Wind damage classifications: <a href="https://www.nhc.noaa.gov/aboutsshws.php" style="color:#888;">'
                'Saffir-Simpson Hurricane Wind Scale</a> (NOAA National Hurricane Center).</p>'
                '<div style="border-top:1px solid #e8e8e8; margin:18px 0;"></div>'

                '<h3 style="color:#1CABE2; border-left:4px solid #1CABE2; padding-left:10px; margin:0 0 12px; font-size:1em; text-transform:uppercase; letter-spacing:0.5px;">'
                'Expected Impact — Storm-Force Winds (50kt)</h3>'
                + impact_bullets + map_section + admin_table + shift_section + timing_box +
                '<div style="border-top:1px solid #e8e8e8; margin:18px 0;"></div>'

                '<h3 style="color:#1CABE2; border-left:4px solid #1CABE2; padding-left:10px; margin:0 0 12px; font-size:1em; text-transform:uppercase; letter-spacing:0.5px;">'
                'Impact by Wind Speed</h3>'
                + thresh_table +
                f'<p style="font-size:0.88em; color:#888; margin-top:4px;">(All values from ECMWF ensemble forecast. {lbl("data")})</p>'
                '<p style="color:#666; margin-top:4px;">At higher wind speeds, fewer areas are affected but those that are '
                'face significantly stronger conditions. The highlighted row shows the storm-force threshold used in the section above.</p>'
                '</div>'

                '<div style="background:#f5f5f5; border-top:1px solid #ddd; padding:14px 24px;">'
                '<p style="font-size:0.88em; color:#666; margin:0 0 10px;">'
                '<strong>Forecast Data</strong><br>'
                'Source: ECMWF ensemble forecast<br>'
                f'Forecast issued: {fd_formatted_full}<br>'
                'Wind threshold shown: 50kt</p>'
                '<p style="font-size:0.86em; color:#999; border-top:1px solid #e0e0e0; padding-top:10px; margin:0;">'
                '&#9888; This alert was generated automatically by an AI system based on probabilistic '
                'model outputs, not observed conditions. Numbers reflect expected values across the '
                'forecast ensemble — figures should be carefully reviewed and verified before use.</p>'
                '</div>'
                '<div style="background:#1CABE2; padding:10px 24px;"></div>'
                '</div>'
            )

            subject = f'Storm Alert: {storm_name} — {pair["country_name"]}'

            # ── 5: Cache in ALERT_SENT_LOG ────────────────────────────────────
            session.sql(f"""
                INSERT INTO ALERT_SENT_LOG
                    (TRACK_ID, FORECAST_TIME, COUNTRY_CODE, EMAIL_SUBJECT, EMAIL_BODY, RECIPIENT_COUNT)
                VALUES (
                    '{t_id}', '{t_ft}', '{t_cc}',
                    '{q(subject)}', '{q(email_body)}', 0
                )
            """).collect()

            # ── 6: Find subscribers for this country ──────────────────────────
            recipients = [
                row[0] for row in session.sql(f"""
                    SELECT EMAIL FROM ALERT_SUBSCRIBERS
                    WHERE ACTIVE = TRUE
                      AND (
                        COUNTRY_CODES IS NULL
                        OR ARRAY_CONTAINS('{t_cc}'::VARIANT, COUNTRY_CODES)
                      )
                """).collect()
            ]

            if not recipients:
                alerts.append(f"{pair['track_id']}/{pair['country_code']}: no matching subscribers")
                continue

            # ── 7: Send emails ────────────────────────────────────────────────
            sent         = 0
            safe_subject = q(subject)
            safe_body    = q(email_body)

            for recipient in recipients:
                try:
                    session.sql(f"""CALL SYSTEM$SEND_EMAIL(
                        'AOTS_EMAIL_INTEGRATION',
                        '{q(recipient)}',
                        '{safe_subject}',
                        '{safe_body}',
                        'text/html'
                    )""").collect()
                    sent += 1
                except Exception as e:
                    errors.append(f'Send failed to {recipient}: {e}')

            session.sql(f"""
                UPDATE ALERT_SENT_LOG SET RECIPIENT_COUNT = {sent}
                WHERE TRACK_ID = '{t_id}' AND FORECAST_TIME = '{t_ft}' AND COUNTRY_CODE = '{t_cc}'
            """).collect()

            alerts.append(f"{pair['track_id']}/{pair['country_code']}: sent to {sent}/{len(recipients)}")

        except Exception as e:
            errors.append(f"{pair['track_id']}/{pair['country_code']}: {e}")

    result = 'Alerted: [' + ', '.join(alerts) + ']'
    if errors:
        result += ' | Errors: ' + ' | '.join(errors)
    return result
$$;

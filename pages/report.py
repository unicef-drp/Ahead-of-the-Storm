import dash
import dash_mantine_components as dmc
from dash import Output, Input, State, callback, dcc, html, callback_context, ctx
import os
from jinja2 import Template
import json


from components.ui.header import make_header
from components.ui.footer import footer
from components.config import config

# Configuration: Set to True to use blob storage, False to use local file
USE_BLOB_REPORT = os.getenv('USE_BLOB_REPORT', 'False').lower() == 'true'

# Import and setup data store only if using blob storage
if USE_BLOB_REPORT:
    from components.data.data_store_utils import get_data_store
    giga_store = get_data_store()
    RESULTS_DIR = os.getenv('RESULTS_DIR')
    REPORT_FILE = os.getenv('REPORT_FILE')
    REPORT_TEMPLATE_FILE = os.getenv('REPORT_TEMPLATE_FILE')


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

def refactor_html_str(html_str,d):
    d_refactored = d.copy()

    # Reformat global change
    for key in d.keys():
        if 'change' in key and key not in ["children_change_direction", "children_change", "children_change_perc"]:
            value = d[key]
            d_refactored[key] = format_change(value,key)

    #Reformat admins
    rows_admins_pop_total = []
    for admin_d in d_refactored['rows_admins_pop_total']:
        values = {'admin_name':admin_d['name'],'cci':admin_d['cci']}
        for wind in storm_categories.keys():
            values[f"expected_pop_{wind}"] = admin_d[f"{wind}"]
            values[f"change_pop_{wind}"] = format_change(admin_d[f"change_{wind}"],"")
        rows_admins_pop_total.append(row_admins_pop.format(**values))
    d_refactored['rows_admins_pop_total'] = "\n".join(rows_admins_pop_total)+"\n"

    rows_admins_school = []
    for admin_d in d_refactored['rows_admins_school']:
        values = {'admin_name':admin_d['name'],'cci':admin_d['cci']}
        for wind in storm_categories.keys():
            values[f"expected_pop_{wind}"] = admin_d[f"{wind}"]
            values[f"change_pop_{wind}"] = format_change(admin_d[f"change_{wind}"],"")
        rows_admins_school.append(row_admins_pop.format(**values))
    d_refactored['rows_admins_school'] = "\n".join(rows_admins_school)+"\n"

    rows_admins_infant = []
    for admin_d in d_refactored['rows_admins_infant']:
        values = {'admin_name':admin_d['name'],'cci':admin_d['cci']}
        for wind in storm_categories.keys():
            values[f"expected_pop_{wind}"] = admin_d[f"{wind}"]
            values[f"change_pop_{wind}"] = format_change(admin_d[f"change_{wind}"],"")
        rows_admins_infant.append(row_admins_pop.format(**values))
    d_refactored['rows_admins_infant'] = "\n".join(rows_admins_infant)+"\n"

    rows_schools_winds = []
    for admin_d in d_refactored['rows_schools_winds']:
        values = {'admin_name':admin_d['name']}
        for wind in storm_categories.keys():
            values[f"pois_{wind}"] = admin_d[f"{wind}"]
        rows_schools_winds.append(row_poi_winds.format(**values))
    d_refactored['rows_schools_winds'] = "\n".join(rows_schools_winds)+"\n"    

    rows_hcs_winds = []
    for admin_d in d_refactored['rows_hcs_winds']:
        values = {'admin_name':admin_d['name']}
        for wind in storm_categories.keys():
            values[f"pois_{wind}"] = admin_d[f"{wind}"]
        rows_hcs_winds.append(row_poi_winds.format(**values))
    d_refactored['rows_hcs_winds'] = "\n".join(rows_hcs_winds)+"\n"


    TEMPLATE = Template(html_str)
    html_str_new = TEMPLATE.render(d_refactored)

    return html_str_new


def make_custom_header():
    """Use the standard header which now includes Last Updated timestamp"""
    from components.ui.header import make_header
    return make_header(active_tab="tab-report")

def make_single_page_layout():
    if USE_BLOB_REPORT:
        # Read from blob storage using data store
        report_path = os.path.join(RESULTS_DIR, REPORT_TEMPLATE_FILE)
        
        # Check if file exists in blob storage
        if not giga_store.file_exists(report_path):
            return dmc.Alert(
                f"Impact report not found at {report_path} in blob storage. Please ensure the file exists.",
                title="Report Not Available",
                color="blue",
                variant="light"
            )
        
        # Read from blob storage
        with giga_store.open(report_path, 'r') as f:
            html_str = f.read()

        
    else:
        # Read from local assets folder
        # pages/report.py -> go up one level -> assets/impact-report.html
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        report_path = os.path.join(project_root, 'assets', 'impact-report.html')
        
        # Check if file exists locally
        if not os.path.exists(report_path):
            return dmc.Alert(
                f"Impact report not found at {report_path}. Please ensure the file exists.",
                title="Report Not Available",
                color="blue",
                variant="light"
            )
        
        # Read the HTML file directly from the local filesystem
        with open(report_path, 'r', encoding='utf-8') as f:
            html_str = f.read()

    return html.Iframe(
        srcDoc=html_str,
        style={"width": "100%", "height": "100vh", "border": 0},
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
                style={"height": "calc(100vh - 67px - 80px)", "overflow": "hidden"}
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

@callback(
    Output("iframe", "srcDoc"),
    Input("country-store","data"),
    Input("storm-store","data"),
    Input("date-store","data"),
    State("country-store","data"),
    State("storm-store","data"),
    State("date-store","data"),
    #prevent_initial_call=True
)
def update_iframe(i_country,i_storm,i_date,s_country,s_storm,s_date):

    if s_country and s_storm and s_date:
        file = f"{s_country}_{s_storm}_{s_date}.json"
        filename = os.path.join(RESULTS_DIR, "jsons", file)
        if not giga_store.file_exists(filename):
            return dash.no_update

        report_path = os.path.join(RESULTS_DIR, REPORT_TEMPLATE_FILE)

        # Read the HTML file 
        with giga_store.open(report_path, 'r') as f:
            html_str = f.read()

        # Read the HTML file 
        with giga_store.open(filename, 'r') as f:
            d = json.load(f)

        return refactor_html_str(html_str,d)

    return dash.no_update
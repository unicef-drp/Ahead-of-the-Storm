import dash
import dash_mantine_components as dmc
from dash import Output, Input, State, callback, dcc, html, callback_context, ctx
import os
from jinja2 import Template
import json


from components.ui.header import make_header
from components.ui.footer import footer
from components.config import config
from components.data.data_store_utils import get_data_store

# Always use data store (works for both local filesystem and blob storage)
# For SPCS, this will be LocalDataStore pointing to /datastore
# For blob storage, this will be ADLSDataStore
giga_store = get_data_store()

# Get configuration from environment or config
RESULTS_DIR = config.RESULTS_DIR or os.getenv('RESULTS_DIR') or "project_results/climate/lacro_project"
REPORT_FILE = os.getenv('REPORT_FILE', 'impact-report-current.html')
# Try both possible template file names
REPORT_TEMPLATE_FILE = os.getenv('REPORT_TEMPLATE_FILE') or 'impact-report-template.html'
REPORT_TEMPLATE_FILE_ALT = 'impact-report.html'  # Alternative name to try

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
    # Try to read from data store first (for SPCS or blob storage)
    report_path = os.path.join(RESULTS_DIR, REPORT_TEMPLATE_FILE)
    report_path_alt = os.path.join(RESULTS_DIR, REPORT_TEMPLATE_FILE_ALT)
    
    html_str = None
    
    # Try primary template file name
    if giga_store.file_exists(report_path):
        print(f"Impact report: Found template at {report_path}")
        try:
            with giga_store.open(report_path, 'r') as f:
                html_str = f.read()
        except Exception as e:
            print(f"Error reading report template from data store: {e}")
    # Try alternative template file name
    elif giga_store.file_exists(report_path_alt):
        print(f"Impact report: Found template at alternative path {report_path_alt}")
        try:
            with giga_store.open(report_path_alt, 'r') as f:
                html_str = f.read()
        except Exception as e:
            print(f"Error reading report template from alternative path: {e}")
    
    # Fall back to local assets folder if not found in data store
    if not html_str:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(current_dir)
        local_report_path = os.path.join(project_root, 'assets', 'impact-report.html')
        
        if os.path.exists(local_report_path):
            print(f"Impact report: Using local template from {local_report_path}")
            with open(local_report_path, 'r', encoding='utf-8') as f:
                html_str = f.read()
        else:
            return dmc.Alert(
                f"Impact report template not found at any of: {report_path}, {report_path_alt}, {local_report_path}. Please ensure the file exists.",
                title="Report Not Available",
                color="blue",
                variant="light"
            )

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
        
        print(f"Impact report: Looking for JSON file: {filename}")
        print(f"Impact report: RESULTS_DIR = {RESULTS_DIR}")
        print(f"Impact report: Country={s_country}, Storm={s_storm}, Date={s_date}")
        
        # Debug: List jsons directory if it exists
        jsons_dir = os.path.join(RESULTS_DIR, "jsons")
        print(f"Impact report: Checking jsons directory: {jsons_dir}")
        if giga_store.file_exists(jsons_dir) or os.path.exists(jsons_dir):
            try:
                # Try to list files in jsons directory
                if hasattr(giga_store, 'list_files'):
                    json_files = giga_store.list_files(jsons_dir)
                    print(f"Impact report: Found {len(json_files)} files in jsons directory")
                    # Show files matching the pattern
                    matching = [f for f in json_files if s_country in f and s_storm in f]
                    print(f"Impact report: Files matching pattern: {matching[:5]}")
            except Exception as e:
                print(f"Impact report: Could not list jsons directory: {e}")
        
        # Check if JSON file exists in data store
        if not giga_store.file_exists(filename):
            print(f"Impact report: JSON file not found at {filename}")
            # Try alternative path construction
            alt_filename = os.path.join(RESULTS_DIR, "jsons", file.lower())
            if giga_store.file_exists(alt_filename):
                print(f"Impact report: Found JSON file at alternative path: {alt_filename}")
                filename = alt_filename
            else:
                print(f"Impact report: Also tried alternative path: {alt_filename}")
                return dash.no_update

        # Get the HTML template path - try both possible names
        report_path = os.path.join(RESULTS_DIR, REPORT_TEMPLATE_FILE)
        report_path_alt = os.path.join(RESULTS_DIR, REPORT_TEMPLATE_FILE_ALT)
        
        html_str = None
        
        # Try primary template file name
        if giga_store.file_exists(report_path):
            print(f"Impact report: Found template at {report_path}")
            with giga_store.open(report_path, 'r') as f:
                html_str = f.read()
        # Try alternative template file name
        elif giga_store.file_exists(report_path_alt):
            print(f"Impact report: Found template at alternative path {report_path_alt}")
            with giga_store.open(report_path_alt, 'r') as f:
                html_str = f.read()
        else:
            # Fall back to local assets folder
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            local_report_path = os.path.join(project_root, 'assets', 'impact-report.html')
            
            if os.path.exists(local_report_path):
                print(f"Impact report: Using local template from {local_report_path}")
                with open(local_report_path, 'r', encoding='utf-8') as f:
                    html_str = f.read()
            else:
                print(f"Impact report: HTML template not found at any of: {report_path}, {report_path_alt}, {local_report_path}")
                return dash.no_update

        if not html_str:
            print(f"Impact report: Failed to load HTML template")
            return dash.no_update

        # Read the JSON data file
        try:
            with giga_store.open(filename, 'r') as f:
                d = json.load(f)
            print(f"Impact report: Successfully loaded JSON data from {filename}")
            print(f"Impact report: JSON keys: {list(d.keys())[:10]}...")  # Show first 10 keys
        except Exception as e:
            print(f"Impact report: Error reading JSON file {filename}: {e}")
            import traceback
            print(f"Impact report: Traceback: {traceback.format_exc()}")
            return dash.no_update

        # Generate the report
        try:
            result = refactor_html_str(html_str, d)
            print(f"Impact report: Successfully generated report")
            return result
        except Exception as e:
            print(f"Impact report: Error generating report: {e}")
            import traceback
            print(f"Impact report: Traceback: {traceback.format_exc()}")
            return dash.no_update

    return dash.no_update
from dash import html
import dash
import dash_mantine_components as dmc
import os

from components.ui.header import make_header
from components.ui.footer import footer

# from gigaspatial.core.io import ADLSDataStore
from components.data.data_store_utils import get_data_store

# giga_store = ADLSDataStore()
giga_store = get_data_store()

RESULTS_DIR = os.getenv('RESULTS_DIR')
REPORT_FILE = os.getenv('REPORT_FILE')

dash.register_page(
    __name__, path="/report", name="Impact Report"
)

# page_content = dmc.Container(
#     [
#         dmc.Card(
#             [
#                 dmc.Title(
#                     "Data Quality Report",
#                     order=3
#                 ),
#                 dmc.Table(
#                     striped=True,
#                     highlightOnHover=True,
#                     withTableBorder=True,
#                     withColumnBorders=True,
#                     data={
#                         "head": ["Category", "Details", "Commentary"],
#                         "body":  [
#                             ["Total Schools Validated", "1,234", "🎉 That's more schools than days in 3.38 years!"],
#                             ["Pending Schools", "456", "🤔 Looks like some schools are still hiding from us."],
#                             ["Errors Found", "78", "🛑 Oops! 78 errors. But hey, who’s counting? Oh right... we are."],
#                             ["Most Common Error", "GPS Coordinates Missing", "📍 Schools aren’t supposed to be invisible!"],
#                             ["Top Validator", "Jane Doe", "🥇 Give this hero a gold star (or at least a coffee)."],
#                             ["Fastest Validation Time", "3.5 seconds", "⚡ Fast enough to make Usain Bolt proud."],
#                             ["Slowest Validation Time", "3 hours, 12 minutes", "🐢 Someone was clearly on island time."],
#                             ["Unicorn Schools Found", "1", "🦄 Yes, we found one magical school. It’s hiding at the end of a rainbow."],
#                             ["App Downtime", "2 minutes", "😱 We swear it wasn’t our fault! (Okay, maybe a little.)"],
#                             ["Validator Mood", "Mostly Happy 😊", "We asked, they smiled. Either we're doing great, or they really love free snacks."]
#                         ],
#                     },
#                     mt="md"
#                 )
#             ],
#             shadow='sm',
#             p='lg',
#             radius='xs',
#             style={
#                 'backgroundColor': '#f0f0f0', 
#                 'width': '100%',
#                 'boxShadow': 'none',
#                 'position': 'relative',
#                 'height': 'auto',
#             }
#         )

#     ],
#     style={},
# )

# layout = dmc.AppShell(
#     [
#         dmc.AppShellHeader(make_header(active_tab="tab-report"), px=15, zIndex=2000),
#         dmc.AppShellMain(page_content),
#         dmc.AppShellFooter(footer, zIndex=2000),
#     ],
#     id="app-shell-report",
#     header={"height": "67"},
#     padding="lg",
#     footer={"height": "128"},
# )


def make_custom_header():
    """Use the standard header which now includes Last Updated timestamp"""
    from components.ui.header import make_header
    return make_header(active_tab="report")

def make_single_page_layout():
    report_path = os.path.join(RESULTS_DIR, REPORT_FILE)
    
    # Check if file exists before trying to open it
    if not giga_store.file_exists(report_path):
        # Return a placeholder message if report doesn't exist yet
        return dmc.Alert(
            "Impact report not yet generated. Please run the data pipeline to generate reports.",
            title="Report Not Available",
            color="blue",
            variant="light"
        )
    
    with giga_store.open(report_path, 'r') as f:
        html_str = f.read()

    return html.Iframe(
        #src="../assets/impact-report.html",    # served automatically
        srcDoc=html_str,
        style={"width": "100%", "height": "100vh", "border": 0}
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


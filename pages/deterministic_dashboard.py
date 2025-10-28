from dash import html, dcc, Input, Output, callback
import dash
import dash_mantine_components as dmc
from dash_iconify import DashIconify

# Import centralized configuration
from components.config import config
from components.ui.header import make_header
from components.ui.footer import footer

dash.register_page(
    __name__, path="/deterministic", name="Deterministic Forecast"
)

def make_custom_header():
    """Use the standard header which now includes Last Updated timestamp"""
    return make_header(active_tab="tab-deterministic")

def make_single_page_layout():
    """Create the deterministic forecast dashboard layout"""
    return dmc.Container(
        [
            dmc.Paper(
                [
                    dmc.Group([
                        DashIconify(icon="carbon:weather-station", width=32),
                        dmc.Title("Deterministic Forecast", order=2)
                    ], gap="md"),
                    dmc.Text("View individual hurricane forecast tracks and their deterministic impacts.", 
                             size="sm", c="dimmed", mt="md"),
                    dmc.Alert(
                        "This feature is under development. Coming soon!",
                        title="Work in Progress",
                        color="blue",
                        variant="light",
                        mt="xl",
                        icon=DashIconify(icon="carbon:information", width=20)
                    ),
                ],
                p="xl",
                shadow="sm",
                style={
                    "marginTop": "40px",
                    "textAlign": "center"
                }
            )
        ],
        size="xl",
        style={"marginTop": "20px"}
    )

def make_single_page_appshell():
    """Create appshell with custom header using existing footer and appshell structure"""
    
    return dmc.AppShell(
        [
            dmc.AppShellHeader(make_custom_header(), px=15, zIndex=2000),
            dmc.AppShellMain(
                make_single_page_layout(),
            ),
            dmc.AppShellFooter(footer, zIndex=2000),
        ],
        id="deterministic-shell",
        header={"height": "67"},
        padding="lg",
        footer={"height": "80"},
    )

# Use the single-page appshell
layout = make_single_page_appshell()


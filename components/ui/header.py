"""Fixed top navigation bar: app title, last-updated timestamp, and page tabs."""
import logging
import dash_mantine_components as dmc
from dash_iconify import DashIconify

logger = logging.getLogger(__name__)

def make_header(active_tab="tab-home"):
    # Timestamp is populated by the update_last_updated_header callback in dashboard.py
    # which fires on startup and every 15 min — never frozen at container start time.
    last_updated = dmc.Group([
        dmc.Text("Last Updated:", size="xs", c="white", opacity=0.8),
        dmc.Text("—", id="header-last-updated", size="sm", fw=500, c="white")
    ], align="center", gap="xs")
    
    return dmc.Group(
        [
            dmc.Burger(id="burger-button", opened=False, hiddenFrom="md"),
            dmc.Group(
                # Left side - Text Title
                dmc.Anchor(
                    dmc.Text(
                        "AHEAD OF THE STORM - IMPACT-BASED FORECASTING",
                        size="lg",
                        fw=700,
                        c="white",
                        style={"textDecoration": "none"}
                    ),
                    href="/",
                    style={"textDecoration": "none"},
                ),
                style={"alignItems": "center"},
            ),
            last_updated,  # Center - Last Updated timestamp
            dmc.Group(
                [
                    # Right side - Tabs
                    dmc.Tabs(
                        [
                            dmc.TabsList(
                                [
                                    dmc.Anchor(
                                        dmc.TabsTab(
                                            "Dashboard",
                                            value="tab-home",
                                            leftSection=DashIconify(
                                                icon="carbon:map", height=16
                                            ),
                                        ),
                                        href="/",
                                        style={
                                            "textDecoration": "none",
                                            "color": "inherit", 
                                        },
                                    ),
                                    dmc.Anchor(
                                        dmc.TabsTab(
                                            "Forecast Analysis",
                                            value="tab-analysis",
                                            leftSection=DashIconify(
                                                icon="carbon:analytics", height=16
                                            ),
                                        ),
                                        href="/analysis",
                                        style={
                                            "textDecoration": "none",
                                            "color": "inherit", 
                                        },
                                    ),
                                    dmc.Anchor(
                                        dmc.TabsTab(
                                            "Report",
                                            value="tab-report",
                                            leftSection=DashIconify(
                                                icon="carbon:report", height=16
                                            ),
                                            #disabled=True,
                                            #c="black"
                                        ),
                                        href="/report",
                                        style={
                                            "textDecoration": "none",
                                            "color": "inherit", 
                                        },
                                    ),
                                ],
                                justify="flex-end",
                                style={
                                    "backgroundColor": "transparent",
                                }
                            ),
                        ],
                        id="tabs",
                        value=active_tab,
                        color="#1cabe2",
                        orientation="horizontal",
                        variant="pills",
                        styles={
                            "tab": {
                                "backgroundColor": "transparent",
                                "color": "white",
                                "&:hover": {
                                    "backgroundColor": "#0058AB",  # Darker blue on hover
                                    "color": "white",
                                },
                                "&[dataActive]": {
                                    "backgroundColor": "#0058AB",  # Active state blue
                                    "color": "white",
                                }
                            },
                            "list": {
                                "backgroundColor": "transparent",
                            }
                        }
                    ),
                ]
            ),
        ],
        justify="space-between",
        style={
            "width": "100%",
            "backgroundColor": "#1cabe2",  # UNICEF Blue to match footer
            "color": "#ffffff",             # White text color
            "padding": "15px 30px",
            "position": "fixed",
            "top": 0,
            "left": 0,
            "zIndex": 1000,
        },
    )

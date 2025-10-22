import dash_mantine_components as dmc
from dash_iconify import DashIconify
from components.data.snowflake_utils import get_latest_forecast_time_overall

def make_header(active_tab="tab-home"):
    # Get the latest forecast time from Snowflake (overall latest, not storm-specific)
    try:
        latest_time = get_latest_forecast_time_overall()
        formatted_time = latest_time.strftime("%b %d, %Y %H:%M UTC") if latest_time else "N/A"
    except Exception as e:
        print(f"Error getting latest forecast time: {e}")
        formatted_time = "N/A"
    
    # Create Last Updated timestamp component
    last_updated = dmc.Group([
        dmc.Text("Last Updated:", size="xs", c="white", opacity=0.8),
        dmc.Text(formatted_time, size="sm", fw=500, c="white")
    ], align="center", gap="xs")
    
    return dmc.Group(
        [
            dmc.Burger(id="burger-button", opened=False, hiddenFrom="md"),
            dmc.Group(
                # Left side - Logo
                dmc.Anchor(
                    dmc.Image(
                        src="assets/img/aots-logo.png",
                        w=240,
                        style={"flex": 1},  # Ensures logo takes up space on the left
                    ),
                    href="/",
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
                                    dmc.Menu(
                                        [
                                            # dmc.MenuTarget(dmc.TabsTab("Account", value="tab-account", leftSection=DashIconify(icon="carbon:user", height=16)),),
                                            dmc.MenuTarget(
                                            dmc.Button(
                                                "Account",
                                                variant="subtle",
                                                c="white",
                                                leftSection=DashIconify(
                                                    icon="carbon:user", height=16
                                                ),
                                            ),
                                            ),
                                            dmc.MenuDropdown(
                                                [
                                                    dmc.MenuItem(
                                                        "Admin Panel",
                                                        href="https://www.github.com/",
                                                        target="_blank",
                                                        leftSection=DashIconify(
                                                            icon="radix-icons:external-link"
                                                        ),
                                                    ),
                                                    dmc.MenuItem(
                                                        "Logout",
                                                        id="logout-button",
                                                        n_clicks=0,
                                                    ),
                                                ]
                                            ),
                                        ],
                                        trigger="hover",
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
                                "&[data-active]": {
                                    "backgroundColor": "#0058AB",  # Active state blue
                                    "color": "white",
                                }
                            },
                            "list": {
                                "backgroundColor": "transparent",
                            }
                        }
                    ),
                    dmc.Menu(
                        [
                            # dmc.MenuTarget(dmc.TabsTab("Account", value="tab-account", leftSection=DashIconify(icon="carbon:user", height=16)),),
                            dmc.MenuTarget(
                                dmc.ActionIcon(
                                    DashIconify(icon="carbon:translate", width=25),
                                    variant="transparent",
                                    c="white",
                                )
                            ),
                            dmc.MenuDropdown(
                                [
                                    dmc.MenuItem(
                                        "English",
                                        id="translate-english",
                                        color="#1cabe2",
                                        n_clicks=0,
                                    ),
                                    dmc.MenuItem(
                                        "French",
                                        id="translate-french",
                                        disabled=True,
                                        n_clicks=0,
                                    ),
                                    dmc.MenuItem(
                                        "Spanish",
                                        id="translate-spanish",
                                        disabled=True,
                                        n_clicks=0,
                                    ),
                                ]
                            ),
                        ],
                        trigger="hover",
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

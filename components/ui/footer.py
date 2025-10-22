import dash_mantine_components as dmc
from dash_iconify import DashIconify

footer = dmc.Group(
    [
        # Left side - Clickable logos with more spacing
        dmc.Group(
            [
                dmc.Anchor(
                    dmc.Image(
                        src="assets/img/FDN-UNICEF-logo_white.png",
                        w=200,
                    ),
                    href="http://frontierdatanetwork.org/",
                    target="_blank",
                    style={"marginRight": "40px"},
                ),
                dmc.Anchor(
                    dmc.Image(
                        src="assets/img/GIGA_lockup_white_horizontal.webp",
                        w=200,
                    ),
                    href="https://giga.global",
                    target="_blank",
                ),
            ],
            align="center",
            gap="lg",
        ),
        # Right side - GitHub icon only
        dmc.Group(
            [
                dmc.Anchor(
                    dmc.ActionIcon(
                        DashIconify(icon="carbon:logo-github", width=32),
                        variant="transparent",
                        style={"color": "#ffffff"},
                    ),
                    href="https://github.com/unicef-drp/TC-ECMWF-Forecast-Pipeline.git",
                    target="_blank",
                ),
            ],
            align="center",
        ),
    ],
    justify="space-between",
    style={
        "width": "100%",
        "height": 80,  # Reduced height from 128 to 80
        "backgroundColor": "#1cabe2",  # UNICEF Blue
        "color": "#ffffff",
        "padding": "15px 30px",
        "position": "fixed",
        "bottom": 0,
        "left": 0,
        "zIndex": 1000,
    },
)

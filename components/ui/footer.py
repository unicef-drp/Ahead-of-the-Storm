import dash_mantine_components as dmc
from dash_iconify import DashIconify

footer = dmc.Group(
    [
        # Left side - Supported by text and clickable logos
        dmc.Group(
            [
                dmc.Text(
                    "Supported by",
                    size="sm",
                    c="white",
                    opacity=0.8,
                    style={"marginRight": "15px"}
                ),
                dmc.Anchor(
                    dmc.Image(
                        src="assets/img/FDN-UNICEF-logo_white.png",
                        w=150,
                    ),
                    href="http://frontierdatanetwork.org/",
                    target="_blank",
                    style={"marginRight": "30px"},
                ),
                dmc.Anchor(
                    dmc.Image(
                        src="assets/img/GIGA_lockup_white_horizontal.webp",
                        w=150,
                    ),
                    href="https://giga.global",
                    target="_blank",
                    style={"marginRight": "30px"},
                ),
                dmc.Anchor(
                    dmc.Image(
                        src="assets/img/OoI_logo.png",
                        w=120,
                    ),
                    href="https://www.unicef.org/innovation/",
                    target="_blank",
                ),
            ],
            align="center",
            gap="md",
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
                    href="https://github.com/unicef-drp/Ahead-of-the-Storm",
                    target="_blank",
                ),
            ],
            align="center",
        ),
    ],
    justify="space-between",
    style={
        "width": "100%",
        "height": 79,  # Adjusted height for smaller logos
        "backgroundColor": "#1cabe2",  # UNICEF Blue
        "color": "#ffffff",
        "padding": "15px 30px",
        "position": "fixed",
        "bottom": 0,
        "left": 0,
        "zIndex": 1000,
    },
)

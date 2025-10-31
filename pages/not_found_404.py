from dash import html
import dash
import dash_mantine_components as dmc

from components.ui.footer import footer

dash.register_page(__name__, name="oops!")

header = dmc.Group(
    style={
        "alignItems": "center",
        "width": "100%",
        "backgroundColor": "#1cabe2",  # UNICEF blue background for header
        "color": "#ffffff",  # White text color
        "padding": "15px 30px",
        "position": "fixed",
        "top": 0,
        "left": 0,
        "zIndex": 1000,
    },
)

page_content = dmc.Center(
    [
        dmc.Stack(
            [
                dmc.Text("404", size="96px", c="white", fw=700),
                dmc.Group(
                    dmc.Image(
                        radius="md",
                        src="https://clipart-library.com/images_k/finding-dory-silhouette/finding-dory-silhouette-15.png",
                        h=400,
                        mt=-20,
                    ),
                ),
                dmc.Text(
                    "Looks like this page is as lost as Nemo!",
                    size="32px",
                    c="white",
                    fw=700,
                    ta="center",
                    mt=-20,
                ),
                dmc.Text(
                    "Swim back to the homepage and help us to search for the right location.",
                    size="20px",
                    c="white",
                    fw=400,
                    mt=5,
                ),
                dmc.Space(h=20),
                dmc.Anchor(
                    dmc.Button(
                        "Let's Go Home",
                        variant="gradient",
                        gradient={"from": "orange", "to": "red"},
                    ),
                    href="/",
                ),
            ],
            align="center",
            gap="sm",
        )
    ],
    style={"backgroundColor": "#1cabe2"},
)

layout = dmc.AppShell(
    [
        dmc.AppShellHeader(header, px=15, zIndex=2000),
        dmc.AppShellMain(page_content, style={"backgroundColor": "#1cabe2"}),
        dmc.AppShellFooter(footer, zIndex=2000),
    ],
    header={"height": "67"},
    padding="lg",
    footer={"height": "128"},
    id="app-shell-404",
)

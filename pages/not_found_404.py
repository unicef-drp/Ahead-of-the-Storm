"""
404 error page for the Ahead of the Storm dashboard.

Displays "page not found" screen with a link back to the homepage.
"""
import dash
import dash_mantine_components as dmc

dash.register_page(__name__, name="oops!")

layout = dmc.Center(
    dmc.Stack(
        [
            dmc.Text("404", size="96px", c="white", fw=700),
            dmc.Text(
                "Looks like this page doesn't exist.",
                size="32px",
                c="white",
                fw=700,
                ta="center",
            ),
            dmc.Text(
                "Head back to the homepage to find what you're looking for.",
                size="20px",
                c="white",
                fw=400,
                mt=5,
            ),
            dmc.Space(h=20),
            dmc.Anchor(
                dmc.Button("Let's Go Home", variant="white", color="#1cabe2"),
                href="/",
            ),
        ],
        align="center",
        gap="sm",
    ),
    style={"backgroundColor": "#1cabe2", "height": "100vh", "width": "100%"},
)

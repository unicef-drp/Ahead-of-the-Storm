from dash import html
import dash
import dash_mantine_components as dmc

from components.ui.header import make_header
from components.ui.footer import footer

dash.register_page(
    __name__, path="/report", name="Data Quality Report - GigaValidate"
)

page_content = dmc.Container(
    [
        dmc.Card(
            [
                dmc.Title(
                    "Data Quality Report",
                    order=3
                ),
                dmc.Table(
                    striped=True,
                    highlightOnHover=True,
                    withTableBorder=True,
                    withColumnBorders=True,
                    data={
                        "head": ["Category", "Details", "Commentary"],
                        "body":  [
                            ["Total Schools Validated", "1,234", "🎉 That's more schools than days in 3.38 years!"],
                            ["Pending Schools", "456", "🤔 Looks like some schools are still hiding from us."],
                            ["Errors Found", "78", "🛑 Oops! 78 errors. But hey, who’s counting? Oh right... we are."],
                            ["Most Common Error", "GPS Coordinates Missing", "📍 Schools aren’t supposed to be invisible!"],
                            ["Top Validator", "Jane Doe", "🥇 Give this hero a gold star (or at least a coffee)."],
                            ["Fastest Validation Time", "3.5 seconds", "⚡ Fast enough to make Usain Bolt proud."],
                            ["Slowest Validation Time", "3 hours, 12 minutes", "🐢 Someone was clearly on island time."],
                            ["Unicorn Schools Found", "1", "🦄 Yes, we found one magical school. It’s hiding at the end of a rainbow."],
                            ["App Downtime", "2 minutes", "😱 We swear it wasn’t our fault! (Okay, maybe a little.)"],
                            ["Validator Mood", "Mostly Happy 😊", "We asked, they smiled. Either we're doing great, or they really love free snacks."]
                        ],
                    },
                    mt="md"
                )
            ],
            shadow='sm',
            p='lg',
            radius='xs',
            style={
                'backgroundColor': '#f0f0f0', 
                'width': '100%',
                'boxShadow': 'none',
                'position': 'relative',
                'height': 'auto',
            }
        )

    ],
    style={},
)

layout = dmc.AppShell(
    [
        dmc.AppShellHeader(make_header(active_tab="tab-report"), px=15, zIndex=2000),
        dmc.AppShellMain(page_content),
        dmc.AppShellFooter(footer, zIndex=2000),
    ],
    id="app-shell-report",
    header={"height": "67"},
    padding="lg",
    footer={"height": "128"},
)
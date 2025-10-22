import dash_mantine_components as dmc

from .footer import footer
from .header import make_header


def make_default_appshell(navbar_content, page_content, id="app-shell", active_tab="tab-home", navbar_width=375):
    return dmc.AppShell(
        [
            dmc.AppShellHeader(make_header(active_tab=active_tab), px=15, zIndex=2000),
            dmc.AppShellNavbar(navbar_content, p=24),
            dmc.AppShellMain(page_content),
            dmc.AppShellFooter(footer, zIndex=2000),
        ],
        id=id,
        header={"height": "67"},
        padding="lg",
        navbar={
            "height": "auto",
            "width": navbar_width,
            "breakpoint": "md",
            "collapsed": {"mobile": True},
        },
        footer={"height": "80"},
    )

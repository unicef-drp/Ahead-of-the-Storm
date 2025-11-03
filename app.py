import dash
import dash_mantine_components as dmc
from dash import Dash, _dash_renderer, dcc, callback, Input, Output, State, dcc
from flask_caching import Cache
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

_dash_renderer._set_react_version("18.2.0")

app = Dash(
    __name__,
    meta_tags=[
        {"name": "AoS Hurricane Impact", "content": "width=device-width, initial-scale=1"}
    ],
    external_stylesheets=dmc.styles.ALL,
    use_pages=True,
)

app.config["suppress_callback_exceptions"] = True
app.title = "AoS Hurricane Impact"
app._favicon = "img/aots_icon.png"
server = app.server


app.layout = dmc.MantineProvider(
    [
        dash.page_container,
        dcc.Store("country-store",data=""),
        dcc.Store("storm-store",data=""),
        dcc.Store("date-store",data=""),
    ],
    id="mantine-provider",
    forceColorScheme="light",
    theme={"fontFamily": "'Open Sans', sans-serif"},
)


@callback(
    Output("app-shell", "navbar"),
    Input("burger-button", "opened"),
    State("app-shell", "navbar"),
    prevent_initial_callback=True,
)
def navbar_is_open(opened, navbar):
    navbar["collapsed"] = {"mobile": not opened}
    return navbar


# app.clientside_callback(
#     """function(admin1_value, hideout){
#         if (!admin1_value){
#             return {filter: [], selected: []};
#         }
#         return {filter: [admin1_value], selected: []};
#     }
#     """,
#     Output("admin2-json", "hideout"),
#     Input("admin1-select", "value"),
#     State("admin2-json", "hideout"),
# )



# toggle_select = """
# function(n_clicks, feature, hideout) {
#     console.log("Hideout state before toggle:", hideout);
#     // Only execute if the layer is clicked
#     if (n_clicks === 0) {
#         return hideout; // Return current selection if no clicks yet
#     }

#     // Use an empty array if hideout.selected is undefined
#     let selected = hideout.selected || [];
#     const admin2_id = feature.properties.admin2_id_giga;

#     // Check if the id is already in the selected array
#     if (selected.includes(admin2_id)) {
#         // If it exists, remove it
#         selected = selected.filter((item) => item !== admin2_id);
#     } else {
#         // If it doesn't exist, add it
#         selected.push(admin2_id);
#     }

#     // Return the updated selected list
#     return {filter: hideout.filter, selected: selected};
# }
# """
# app.clientside_callback(
#     toggle_select,
#     Output("admin2-json", "hideout", allow_duplicate=True),
#     Input("admin2-json", "n_clicks"),
#     State("admin2-json", "clickData"),
#     State("admin2-json", "hideout"),
#     prevent_initial_call=True,
# )

if __name__ == "__main__":
    app.run(debug=True)

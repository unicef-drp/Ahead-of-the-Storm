#!/usr/bin/env python3
"""
Impact Analysis Script

This script performs hurricane impact analysis by reading hurricane envelope data directly 
from Snowflake and creating impact analysis views for schools, health centers, population, 
and infrastructure. The analysis uses geospatial intersection calculations to determine 
potential impacts on critical infrastructure.

Key Features:
- Reads hurricane envelope data directly from Snowflake TC_ENVELOPES_COMBINED table
- Performs geospatial intersection analysis with infrastructure data
- Creates impact views for multiple wind speed thresholds
- Supports multiple countries and flexible storage backends (local/blob)
- Generates comprehensive impact statistics and visualizations

Usage:
    python impact_analysis.py --storm JERRY --date "20251010000000"
    python impact_analysis.py --storm JERRY --date "20251010000000" --countries DOM
"""

import os
import sys
import argparse
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import box
from pyproj import CRS
from shapely import union_all
from shapely import wkt as shapely_wkt

# Import centralized configuration
from .config import config

# Import GigaSpatial components
from gigaspatial.core.io import DataStore
from gigaspatial.handlers import AdminBoundaries, RWIHandler
from gigaspatial.processing import convert_to_geodataframe, buffer_geodataframe
from gigaspatial.handlers import GigaSchoolLocationFetcher
from gigaspatial.generators import GeometryBasedZonalViewGenerator, MercatorViewGenerator
from gigaspatial.handlers.healthsites import HealthSitesFetcher
from gigaspatial.core.io.readers import read_dataset
from gigaspatial.core.io.writers import write_dataset

# Import centralized data store utility
from data_store_utils import get_data_store

# Import only Snowflake data retrieval functions
from snowflake_utils import (
    get_envelopes_from_snowflake,
    convert_envelopes_to_geodataframe
)

##### Constants #####
# oecs_countries = ['ATG','DMA','GRD','MSR','KNA','LCA','VCT','AIA','VGB']
# countries = ['NIC','DOM','BLZ'] + oecs_countries
default_countries = ['NIC']
#####################

##### Environment Variables #####
RESULTS_DIR = config.RESULTS_DIR
BBOX_FILE = config.BBOX_FILE
VIEWS_DIR = config.VIEWS_DIR
ROOT_DATA_DIR = config.ROOT_DATA_DIR
#################################

# Initialize data store using centralized utility
data_store = get_data_store()



# =============================================================================
# GEOSPATIAL PROCESSING FUNCTIONS
# =============================================================================

def rectangle_bbox_padded(geom, padding_km=0, crs_in=4326, proj_mode="utm"):
    """
    Smallest axis-aligned rectangle in meters that contains `geom`, padded by `padding_km`,
    returned as a lon/lat (EPSG:4326) polygon + its bounds tuple.

    geom: shapely (Multi)Polygon/Polygon
    padding_km: pad equally on all sides (km)
    crs_in: EPSG of `geom` (default 4326)
    proj_mode: "aeqd" (local Azimuthal Equidistant, good general choice) or "utm"
    """
    gs = gpd.GeoSeries([geom], crs=f"EPSG:{crs_in}")

    # choose a local metric CRS
    if proj_mode == "utm":
        proj = gs.estimate_utm_crs()
    else:  # AEQD centered on the geometry
        c = gs.unary_union.centroid
        proj = CRS.from_proj4(f"+proj=aeqd +lat_0={c.y} +lon_0={c.x} +datum=WGS84 +units=m +no_defs")

    gm = gs.to_crs(proj).iloc[0]
    minx, miny, maxx, maxy = gm.bounds
    pad = float(padding_km) * 1000.0

    rect_m = box(minx - pad, miny - pad, maxx + pad, maxy + pad)
    rect_ll = gpd.GeoSeries([rect_m], crs=proj).to_crs(crs_in).iloc[0]
    return rect_ll

def get_country_boundaries(countries):
    """
    countries: list of iso3 codes for countries of interest
    returns: list of admin 0 boundaries
    """
    country_boundaries = []
    for country in countries:
        admin_boundaries = AdminBoundaries.create(country_code=country, admin_level=0)
        country_boundaries.append(admin_boundaries.to_geodataframe().geometry.iat[0])
    return country_boundaries

def get_padded_bounding_box_for_countries(countries, padding_km=1000):
    """
    countries: list of iso3 codes for countries of interest
    padding_km: padding to add on all sides (in kilometers)
    returns: bbox_polygon_lonlat
    """
    country_boundaries = get_country_boundaries(countries)
    gs = gpd.GeoSeries(country_boundaries, crs='EPSG:4326')
    u = union_all(gs)
    return rectangle_bbox_padded(u, padding_km)

def is_envelope_in_zone(bbox, df_envelopes, geometry_column='geometry'):
    """
    bbox: a polygon representing a bbox
    df_envelopes: DataFrame or GeoDataframe with the envelopes of the tracks
    returns: bool whether any of the envelopes intersects with bbox
    """
    if df_envelopes.empty:
        return False

    if geometry_column != 'geometry':
        df_envelopes = df_envelopes.rename(columns={geometry_column: 'geometry'})
    
    if isinstance(df_envelopes, pd.DataFrame):
        gdf_envelopes = convert_to_geodataframe(df_envelopes.dropna(subset=['geometry']))
    else:  # GeoDataFrame
        gdf_envelopes = df_envelopes.copy()

    mask = gdf_envelopes.intersects(bbox)
    return bool(mask.any())

def save_bounding_box():
    """
    Save bounding box file so not to recalculate every time
    """
    bbox = get_padded_bounding_box_for_countries(default_countries)
    gbbox = gpd.GeoDataFrame(geometry=[bbox], crs='EPSG:4326')
    write_dataset(gbbox, data_store, os.path.join(RESULTS_DIR, BBOX_FILE))

def read_bounding_box():
    """
    Read bounding box file and return bbox geometry
    """
    # Use absolute path from project root
    #project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    bbox_path = os.path.join(RESULTS_DIR, BBOX_FILE)
    gbbox = read_dataset(data_store, bbox_path)
    return gbbox.geometry.iat[0]

def create_mercator_country_layer(country, zoom_level=15):
    """
    countries: list of iso3 codes of countries
    zoom_level: int - zoom level for mercator tiles
    returns a geodataframe with the mercator view
    """
    #### schools ####
    gdf_schools = GigaSchoolLocationFetcher(country, api_key=os.getenv("GIGA_SCHOOL_LOCATION_KEY")).fetch_locations(process_geospatial=True)
    # seems API is returning old column name...
    if 'giga_id_school' in gdf_schools.columns:
        gdf_schools = gdf_schools.rename(columns={'giga_id_school': 'school_id_giga'})
    #################
        
    #### health centers ####
    try:
        gdf_hcs = HealthSitesFetcher(country=country, api_key=os.getenv("HEALTHSITES_API_KEY")).fetch_facilities(output_format='geojson')
        if gdf_hcs.empty or 'geometry' not in gdf_hcs.columns:
            print(f"No health center data available for {country} in mercator layer")
            gdf_hcs = gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
        else:
            gdf_hcs = gdf_hcs.set_crs(4326)
    except Exception as e:
        print(f"Error fetching health centers for {country}: {str(e)}")
        gdf_hcs = gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
    ########################

    tiles_viewer = MercatorViewGenerator(source=country, zoom_level=zoom_level, data_store=data_store)
    try:
        tiles_viewer.map_wp_pop(country=country, resolution=1000, output_column="school_age_population", school_age=True, project="age_structures", un_adjusted=False, sex='F_M')
    except:
        # this should be ready in the next release
        print("No school age available")
        school_age = {k: np.nan for k in tiles_viewer.view.index.unique()}
        tiles_viewer.add_variable_to_view(school_age, 'school_age_population')

    tiles_viewer.map_wp_pop(country=country, resolution=100)
    tiles_viewer.map_built_s()
    tiles_viewer.map_smod()
    
    ## schools
    schools = tiles_viewer.map_points(points=gdf_schools)
    tiles_viewer.add_variable_to_view(schools, "num_schools")

    ## health centers
    hcs = tiles_viewer.map_points(points=gdf_hcs)
    tiles_viewer.add_variable_to_view(hcs, "num_hcs")

    # should we do RWI?
    try:
        handler = RWIHandler(data_store=data_store)
        rwi_df = handler.load_data(country, ensure_available=True)
        rwi_gdf = convert_to_geodataframe(rwi_df)
        rwi = tiles_viewer.map_points(rwi_gdf, value_columns='rwi', aggregation='mean')
    except:
        print("Relative Wealth Index will be empty")
        rwi = {k: np.nan for k in tiles_viewer.view.index.unique()}
    
    tiles_viewer.add_variable_to_view(rwi, 'rwi')

    gdf_tiles = tiles_viewer.to_geodataframe()
    gdf_tiles.rename(columns={'zone_id': 'tile_id'}, inplace=True)
    
    return gdf_tiles

def create_mercator_layers(countries,zoom_level=15):
    """
    countries: list of iso3 codes of countries
    zoom_level: int - zoom level for mercator tiles
    returns a dictionary of countr - geodataframe
    """
    country_mercators = {}
    for country in countries:
        country_mercators[country] = create_mercator_country_layer(country, zoom_level)

    return country_mercators

def save_mercator_view(gdf, country, zoom_level=15):
    """Save base mercator infrastructure view for country"""
    file_name = f"{country}_{zoom_level}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name))

def load_mercator_view(country, zoom_level=15):
    """Load mercator view for country"""
    file_name = f"{country}_{zoom_level}.parquet"
    return read_dataset(data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name))

def create_school_view_from_envelopes(gdf_schools, gdf_envelopes):
    """
    gdf_schools: a geodataframe with school data
    gdf_envelopes: a geodataframe with envelopes
    returns a dictionary of wind threshold - geodataframe with the corresponding school view
    """
    gdf_schools_buff = buffer_geodataframe(gdf_schools, buffer_distance_meters=150)
    wind_views = {}

    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            num_ensembles = len(list(gdf_envelopes_wth.ensemble_member.unique()))

            schools_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_schools_buff, zone_id_column='school_id_giga')
            try:
                new_col = schools_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except:
                probs = {k: 0.0 for k in schools_viewer.view.index.unique()}
            schools_viewer.add_variable_to_view(probs, 'probability')

            gdf_view = schools_viewer.to_geodataframe()
            wind_views[wind_th] = gdf_view

    return wind_views

def create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes):
    """
    gdf_hcs: a geodataframe with school data
    gdf_envelopes: a geodataframe with envelopes
    returns a dictionary of wind threshold - geodataframe with the corresponding health center view
    """
    gdf_hcs_buff = buffer_geodataframe(gdf_hcs, buffer_distance_meters=150)
    wind_views = {}

    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            num_ensembles = len(list(gdf_envelopes_wth.ensemble_member.unique()))

            hcs_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_hcs_buff, zone_id_column='osm_id')
            try:
                new_col = hcs_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except:
                probs = {k: 0.0 for k in hcs_viewer.view.index.unique()}
            hcs_viewer.add_variable_to_view(probs, 'probability')

            gdf_view = hcs_viewer.to_geodataframe()
            wind_views[wind_th] = gdf_view

    return wind_views

def create_mercator_view_from_envelopes(gdf_tiles, gdf_envelopes):
    """Create mercator tile impact views from envelopes"""
    wind_views = {}
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]
        if not gdf_envelopes_wth.empty:
            num_ensembles = len(list(gdf_envelopes_wth.ensemble_member.unique()))

            tiles_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_tiles, zone_id_column='tile_id')
            try:
                new_col = tiles_viewer.map_polygons(gdf_envelopes_wth)
                probs = {k: v / float(num_ensembles) for k, v in new_col.items()}
            except:
                probs = {k: 0.0 for k in tiles_viewer.view.index.unique()}
            tiles_viewer.add_variable_to_view(probs, 'probability')

            gdf_view = tiles_viewer.to_geodataframe()
            wind_views[wind_th] = gdf_view

    return wind_views

def create_tracks_view_from_envelopes(gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes, index_column='ensemble_member'):
    """Create tracks impact views from envelopes"""
    wind_views = {}
    wind_ths = list(gdf_envelopes.wind_threshold.unique())
    for wind_th in wind_ths:
        gdf_envelopes_wth = gdf_envelopes[gdf_envelopes.wind_threshold == int(wind_th)]

        tracks_viewer = GeometryBasedZonalViewGenerator(zone_data=gdf_envelopes_wth, zone_id_column=index_column)
        
        # Schools
        schools = tracks_viewer.map_points(points=gdf_schools)
        tracks_viewer.add_variable_to_view(schools, "severity_schools")

        # Health centers
        hcs = tracks_viewer.map_points(points=gdf_hcs)
        tracks_viewer.add_variable_to_view(hcs, "severity_hcs")

        # Tiles
        try:
            population = tracks_viewer.map_polygons(polygons=gdf_tiles, value_columns="population", aggregation="sum")
            tracks_viewer.add_variable_to_view(population, "severity_population")
            surface = tracks_viewer.map_polygons(polygons=gdf_tiles, value_columns='built_surface_m2', aggregation="sum")
            tracks_viewer.add_variable_to_view(surface, "severity_built_surface_m2")
        except:
            zeros = {k: 0 for k in gdf_envelopes_wth[index_column].unique()}
            tracks_viewer.add_variable_to_view(zeros, "severity_population")
            tracks_viewer.add_variable_to_view(zeros, "severity_built_surface_m2")

        gdf_view = tracks_viewer.to_geodataframe()
        wind_views[wind_th] = gdf_view

    return wind_views

def save_school_view(gdf, country, storm, date, wind_th):
    """
    Saves school view
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'school_views', file_name))

def save_hc_view(gdf, country, storm, date, wind_th):
    """
    Saves health center views
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', file_name))

def save_hc_locations(gdf, country):
    """
    Saves health center locations
    """
    file_name = f"{country}_health_centers.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', file_name))

def load_hc_locations(country):
    """
    Loads health center locations
    """
    file_name = f"{country}_health_centers.parquet"
    return read_dataset(data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'hc_views', file_name))

def save_tiles_view(gdf, country, storm, date, wind_th, zoom_level=15):
    """
    Saves tiles views
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}_{zoom_level}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'mercator_views', file_name))

def save_tracks_view(gdf, country, storm, date, wind_th):
    """
    Saves tracks views
    """
    file_name = f"{country}_{storm}_{date}_{wind_th}.parquet"
    write_dataset(gdf, data_store, os.path.join(ROOT_DATA_DIR, VIEWS_DIR, 'track_views', file_name))


# =============================================================================

def create_views_from_envelopes_in_country(country, storm, date, gdf_envelopes):
    """
    country: ios3 code of country
    storm: storm name
    date: date of the envelopes forecast
    gdf_envelopes: geodataframe with the forecasted envelopes
    **** Creates and saves the views
    """
    print(f"  Processing {country}...")
    
    # Schools
    print(f"    Processing schools...")
    gdf_schools = GigaSchoolLocationFetcher(country).fetch_locations(process_geospatial=True)
    if 'giga_id_school' in gdf_schools.columns:
        gdf_schools = gdf_schools.rename(columns={'giga_id_school': 'school_id_giga'})
    
    wind_school_views = create_school_view_from_envelopes(gdf_schools, gdf_envelopes)
    for wind_th in wind_school_views:
        save_school_view(wind_school_views[wind_th], country, storm, date, wind_th)
    print(f"    Created {len(wind_school_views)} school views")

    # Health centers
    print(f"    Processing health centers...")
    try:
        gdf_hcs = HealthSitesFetcher(country=country).fetch_facilities(output_format='geojson')
        if gdf_hcs.empty or 'geometry' not in gdf_hcs.columns:
            print("     API issues, trying saved locations")
            try:
                gdf_hcs = load_hc_locations(country)
            except:
                print(f"    No health center data available for {country}, skipping...")
                wind_hc_views = {}
                # Create empty GeoDataFrame with proper geometry column for tracks processing
                gdf_hcs = gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')
        else:
            gdf_hcs = gdf_hcs.set_crs(4326)
            # let's save this because API is flaky
            save_hc_locations(gdf_hcs, country)
            wind_hc_views = create_health_center_view_from_envelopes(gdf_hcs, gdf_envelopes)
            for wind_th in wind_hc_views:
                save_hc_view(wind_hc_views[wind_th], country, storm, date, wind_th)
        print(f"    Created {len(wind_hc_views)} health center views")
    except Exception as e:
        print(f"    Error processing health centers: {str(e)}")
        print(f"    Skipping health center analysis for {country}")
        wind_hc_views = {}
        # Create empty GeoDataFrame with proper geometry column for tracks processing
        gdf_hcs = gpd.GeoDataFrame(columns=['geometry'], crs='EPSG:4326')

    # Tiles
    print(f"    Processing tiles...")
    try:
        gdf_tiles = load_mercator_view(country)
        print(f"    Loaded existing mercator tiles: {len(gdf_tiles)} tiles")
    except:
        print(f"    Creating base mercator tiles for {country}...")
        gdf_tiles = create_mercator_country_layer(country)
        save_mercator_view(gdf_tiles, country)
        print(f"    Created and saved base mercator tiles: {len(gdf_tiles)} tiles")
    
    wind_tiles_views = create_mercator_view_from_envelopes(gdf_tiles, gdf_envelopes)
    for wind_th in wind_tiles_views:
        save_tiles_view(wind_tiles_views[wind_th], country, storm, date, wind_th)
    print(f"    Created {len(wind_tiles_views)} tile views")

    # Tracks
    print(f"    Processing tracks...")
    wind_tracks_views = create_tracks_view_from_envelopes(gdf_schools, gdf_hcs, gdf_tiles, gdf_envelopes, index_column='ensemble_member')
    for wind_th in wind_tracks_views:
        save_tracks_view(wind_tracks_views[wind_th], country, storm, date, wind_th)
    print(f"    Created {len(wind_tracks_views)} track views")

def load_envelopes_from_snowflake(storm, date):
    """Load envelope data directly from Snowflake"""
    # Convert date format if needed
    if len(date) == 14:  # YYYYMMDDHHMMSS format
        # Convert to datetime string format
        dt = pd.to_datetime(date, format="%Y%m%d%H%M%S")
        forecast_time = dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        forecast_time = date
    
    try:
        # Get envelope data from Snowflake
        df_envelopes = get_envelopes_from_snowflake(storm, forecast_time)
        
        if df_envelopes.empty:
            print(f"Error: No envelope data found in Snowflake for {storm} at {forecast_time}")
            return pd.DataFrame()
        
        # Convert to GeoDataFrame
        gdf_envelopes = convert_envelopes_to_geodataframe(df_envelopes)
        return gdf_envelopes
        
    except Exception as e:
        print(f"Error loading envelopes from Snowflake: {str(e)}")
        return pd.DataFrame()
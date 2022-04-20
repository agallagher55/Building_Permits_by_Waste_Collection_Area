"""
March 2022
Run-Time: ~20 minutes
A. Gallagher
Outputs: Excel file
"""

import arcpy
import os
import logging
import pandas as pd

from time import sleep
from datetime import datetime
from configparser import ConfigParser

arcpy.env.overwriteOutput = True

TODAY = datetime.today().date().strftime('%m%d%Y')
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

OUTPUT_EXCEL_NAME = f"Building_Dwellings_Summarized_{TODAY}.xlsx"

# Config Settings from .ini file
parser = ConfigParser()
parser.read("settings.ini")
settings = parser['settings']

SDE = settings.get("SDE")

# FEATURES
BUILDING_POLYGONS_FEATURE = os.path.join(SDE, "SDEADM.BLD_building_polygon")
WASTE_COLLECTION_AREAS_FEATURE = os.path.join(SDE, "SDEADM.ADM_solid_waste", "SDEADM.ADM_waste_coll_area")
BUILDING_USE_TABLE = os.path.join(SDE, "SDEADM.BLD_BUILDING_USE")

# Logger Settings
LOG_FILE = f"logs_{TODAY}.log"

logger = logging.getLogger("__name__")
level = logging.DEBUG
logger.setLevel(level)

FORMATTER = logging.Formatter(
    '%(asctime)s | %(levelname)s | FUNCTION: %(funcName)s | Msgs: %(message)s', datefmt='%d-%b-%y %H:%M:%S'
)

handler = logging.FileHandler(LOG_FILE)
handler.setFormatter(FORMATTER)
logger.addHandler(handler)


def export_report(aggregated_df: pd.DataFrame, raw_df: pd.DataFrame, excel_file=OUTPUT_EXCEL_NAME):
    """
    - Export report to excel
    :param aggregated_df: Summary of dwelling units by Collection Area
    :param raw_df: All dwelling units in each collectino area
    :param excel_file: name of excel file - will be output to script directory
    :return:
    """

    # Check input reports for expected results
    summary_rows = len(raw_df.index)  # Number of rows in aggregated/summary report
    area_1_units = aggregated_df.loc["AREA 1", "SUM of DWELLING UNITS"]  # Number of dwelling units in Area 1

    arcpy.AddMessage(f"# of Summary Rows: {summary_rows}, Expected: ~130k")
    arcpy.AddMessage(f"# of Area 1 Units: {area_1_units}, Expected: ~34k")

    if summary_rows < 130000:
        logger.warning(f"# of Summary Rows: {summary_rows}, Expected: ~130k")
    else:
        logger.info(f"# of Summary Rows: {summary_rows}, Expected: ~130k")

    if area_1_units < 30000:
        logger.warning(f"# of Area 1 Units: {area_1_units}, Expected: ~34k")
    else:
        logger.info(f"# of Area 1 Units: {area_1_units}, Expected: ~34k")

    # EXPORT TO EXCEL
    if len(raw_df.index) > 0:
        arcpy.AddMessage("\nExporting Report to Excel...")

        with pd.ExcelWriter(excel_file) as writer:
            aggregated_df.to_excel(writer, sheet_name="final")
            raw_df.to_excel(writer, sheet_name="summary")

        logger.debug("Exported to Excel")
        return excel_file


def create_report(dwellings_table) -> (pd.DataFrame, pd.DataFrame):
    """
    - Convert table to DataFrame and aggregate data by Collection Area, Number of Dwelling Units
    - Filter out any buildings with more than 6 units
    - Append a row to sum number of dwelling units

    :param dwellings_table: Table with BL_ID, DWELLING UNITS, COLLECTION AREA data
    :return: DataFrames - 1) Raw Data, 2) Aggregated by Collection Area Summary Table
    """

    dwel_units_sql = "DWEL_UNITS <= 6"

    case_fields = ["COLL_AREA", "BL_ID"]  # First aggregate data by these fields
    dataframe_columns = ["BL_ID", "COLL_AREA", "DWEL_UNITS"]

    arcpy.AddMessage("\nSummarizing Attributes...")
    logger.debug("Summarizing Attributes...")

    # Convert table to Numpy Array in order to create DataFrame
    np_array = arcpy.da.FeatureClassToNumPyArray(
        dwellings_table,
        field_names=dataframe_columns,  # Set columns of interest
        null_value={"DWEL_UNITS": 0}  # Translate NULL values to zero
    )
    df = pd.DataFrame(np_array)

    # Get SUM and COUNT of DWELLING UNITS, BL_IDs
    print(f"\tGrouping by {', '.join(case_fields)}...")
    df_summary_one = df.query(dwel_units_sql).groupby(case_fields).agg({"DWEL_UNITS": "sum", "BL_ID": "count"})

    print(f"\tGrouping by COLL_AREA")
    df_final = df_summary_one.groupby("COLL_AREA").agg({"DWEL_UNITS": "sum", "BL_ID": "count"})

    # Add sum row
    df_final.loc["TOTAL"] = df_final.sum()

    df_final.rename(
        columns={"DWEL_UNITS": "SUM of DWELLING UNITS", "BL_ID": "BL_ID COUNT"},
        index={"COLL_AREA": "COLLECTION AREA"},
        inplace=True
    )

    print(df_final)
    return df_final, df_summary_one


def dwelling_units():
    """
    - Main process. Run geospatial analysis
    :return:
    """

    # Create GEODATABASE and Use for as workspace
    working_gdb = arcpy.CreateFileGDB_management(
        out_folder_path=SCRIPT_DIR,
        out_name="temp_workspace.gdb"
    )[0]  # This will overwrite any existing gdb, clearing any existing feature classes

    # DISSOLVE BUILDING POLYGONS
    arcpy.AddMessage("\nDissolving Building Polygons...")
    logger.debug("Dissolving Building Polygons...")
    dissolved_building_polygons = arcpy.Dissolve_management(
        in_features=BUILDING_POLYGONS_FEATURE,
        out_feature_class=os.path.join(working_gdb, "dissolved_building_polygons"),
        dissolve_field=["BL_ID"],
        statistics_fields=[],
        multi_part="MULTI_PART",
        unsplit_lines="DISSOLVE_LINES"
    )[0]

    # SPATIAL JOIN DISSOLVED BUILDING POLYGONS WITH WASTE COLLECTION AREAS
    arcpy.AddMessage("\nSpatially Joining Building Polygons and Waste Collection Areas...")
    logger.debug("Spatially Joining Building Polygons and Waste Collection Areas...")
    buildings_waste_areas = arcpy.SpatialJoin_analysis(
        target_features=dissolved_building_polygons,
        join_features=WASTE_COLLECTION_AREAS_FEATURE,
        out_feature_class=os.path.join(working_gdb, "buildings_w_waste_areas"),
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        match_option="INTERSECT",
        search_radius="",
        distance_field_name=""
    )[0]

    # JOIN BUILDING USE TABLE TO BUILDING POLYGONS
    arcpy.AddMessage("\nJoining Building Use Table to Building Polygons...")
    building_polygons_w_bld_use = arcpy.JoinField_management(
        in_data=buildings_waste_areas,
        in_field="BL_ID",
        join_table=BUILDING_USE_TABLE,
        join_field="BL_ID",
        fields=[]
    )[0]

    sleep(10)  # Ensure join geoprocessing tool is finished.

    # TODO: Export Joined Feature (for debug only)
    arcpy.AddMessage("\tExporting Joined Features...")
    joined_feature = arcpy.FeatureClassToFeatureClass_conversion(
        in_features=building_polygons_w_bld_use,
        out_path=working_gdb,
        out_name="buildings_w_waste_areas_blduse",
        where_clause="Join_Count > 0"  # Dwel Units <=6
    )[0]

    joined_records_count = int(arcpy.GetCount_management(joined_feature)[0])
    arcpy.AddMessage(f"\tNumber of Joined Records: {joined_records_count}. (Should be >100k)")

    if joined_records_count < 100000:
        logger.warning(f"Number of Joined Records: {joined_records_count}. (Should be >100k)")
    else:
        logger.info(f"Number of Joined Records: {joined_records_count}. (Should be >100k)")

    return joined_feature


if __name__ == "__main__":
    arcpy.env.overwriteOutput = True

    try:
        dwelling_units = dwelling_units()
        df_final, df_summarized = create_report(dwellings_table=dwelling_units)
        export_report(df_final, df_summarized)

    except arcpy.ExecuteError:
        msg = f"ARCPY ERROR: {arcpy.GetMessages()}"

        logger.error(msg)
        print(msg)

    except Exception as e:
        print(e)
        logger.error(e)

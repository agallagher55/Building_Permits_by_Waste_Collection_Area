import arcpy
import os
import logging
import pandas as pd

from time import sleep
from datetime import datetime
from configparser import ConfigParser

arcpy.env.overwriteOutput = True
arcpy.SetLogHistory(False)

TODAY = datetime.today().date().strftime('%m%d%Y')
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

OUTPUT_EXCEL_NAME = f"Building_Dwellings_Summarized_{TODAY}.xlsx"
EXCEL_PATH = os.path.join(SCRIPT_DIR, OUTPUT_EXCEL_NAME)
LOG_FILE = f"logs_{TODAY}.log"
LOGGING_LEVEL = "debug"

# Config Settings from ini file
parser = ConfigParser()
parser.read("settings.ini")
settings = parser['settings']

WORKING_DIR = settings.get("WORKING_DIR")
SDE = settings.get("SERVER_SDE")

DWEL_UNITS_SQL = "SUM_DWEL_UNITS <= 6 And SUM_DWEL_UNITS <> 0"

# Logger Settings
FORMATTER = logging.Formatter(
    '%(asctime)s | %(levelname)s | FUNCTION: %(funcName)s | Msgs: %(message)s', datefmt='%d-%b-%y %H:%M:%S'
)

levels = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL
}
level = levels[LOGGING_LEVEL.lower()]

handler = logging.FileHandler(LOG_FILE)
handler.setFormatter(FORMATTER)

logger = logging.getLogger("__name__")
logger.setLevel(level)
logger.addHandler(handler)


def summarize_units(dwellings_table) -> (pd.DataFrame, pd.DataFrame):
    """
    - Convert table to DataFrame and aggregate data by Collection Area, Number of Dwelling Units
    - Filter out any buildings with more than 6 units
    - Append a row to sum number of dwelling units

    :param dwellings_table: Table with BL_ID, DWELLING UNITS, COLLECTION AREA data
    :return: DataFrames - 1) Raw Data, 2) Aggregated by Collection Area Summary Table
    """

    print("\nSummarizing Units...")
    case_fields = ["COLL_AREA", "BL_ID"]  # First aggregate data by these fields
    dataframe_columns = ["BL_ID", "COLL_AREA", "DWEL_UNITS"]

    # Convert table to Numpy Array in order to create DataFrame
    np_array = arcpy.da.FeatureClassToNumPyArray(
        dwellings_table,
        field_names=dataframe_columns,  # Set columns of interest
        null_value={"DWEL_UNITS": 0}  # Translate NULL values to zero
    )
    df = pd.DataFrame(np_array)

    # Get SUM and COUNT of DWELLING UNITS, BL_IDs
    print(f"\tGrouping by {', '.join(case_fields)}...")
    df_summary_one = df.query("DWEL_UNITS <= 6").groupby(case_fields).agg({"DWEL_UNITS": "sum", "BL_ID": "count"})

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

    # Export DataFrames to csv
    for df_info in (df_final, "df_final.csv"), (df_summary_one, "df_summary.csv"), (df, "df_original.csv"):
        dataframe, file_name = df_info
        dataframe.to_csv(file_name, index=False)

    return df_final, df_summary_one


def waste_analysis(workspace, building_polygons, building_use, collection_areas):
    """
    The waste_analysis function performs the following tasks:
        1. Dissolves building polygons by BL_ID
        2. Spatially joins dissolved building polygons with waste collection areas (one-to-one)
        3. Joins Building Use table to spatially joined feature class (building polygon and waste area) on BL_ID field
            - This is done to get the number of dwelling units per building, which is needed for calculating
              total number of dwelling units in each collection area

    :param workspace: Set the workspace for the script
    :param building_polygons: Dissolve the building polygons
    :param building_use: Join the building use table to the building polygons
    :param collection_areas: Spatially join the building polygons with the waste collection areas
    """

    # DISSOLVE BUILDING POLYGONS
    print("\nDissolving Building Polygons...")
    logger.debug("\nDissolving Building Polygons...")
    dissolved_building_polygons = arcpy.Dissolve_management(
        in_features=building_polygons,
        out_feature_class=os.path.join(workspace, "dissolved_building_polygons"),
        dissolve_field=["BL_ID"],
        statistics_fields=[],
        multi_part="MULTI_PART",
        unsplit_lines="DISSOLVE_LINES"
    )[0]

    # SPATIAL JOIN DISSOLVED BUILDING POLYGONS WITH WASTE COLLECTION AREAS
    print("\nSpatially Joining Building Polygons and Waste Collection Areas...")
    logger.debug("\nSpatially Joining Building Polygons and Waste Collection Areas...")
    buildings_waste_areas = arcpy.SpatialJoin_analysis(
        target_features=dissolved_building_polygons,
        join_features=collection_areas,
        out_feature_class=os.path.join(workspace, "buildings_w_waste_areas"),
        join_operation="JOIN_ONE_TO_ONE",
        join_type="KEEP_ALL",
        match_option="INTERSECT",
        search_radius="",
        distance_field_name=""
    )[0]

    # JOIN BUILDING USE TABLE TO BUILDING POLYGONS
    print("\nJoining Building Use Table to Building Polygons...")
    building_polygons_w_bld_use = arcpy.JoinField_management(
        in_data=buildings_waste_areas,
        in_field="BL_ID",
        join_table=building_use,
        join_field="BL_ID",
        fields=[]
    )[0]

    sleep(10)

    # TODO: Export Joined Feature (for debug only)
    print("\tExporting Joined Features...")
    joined_feature = arcpy.FeatureClassToFeatureClass_conversion(
        in_features=building_polygons_w_bld_use,
        out_path=workspace,
        out_name="buildings_w_waste_areas_blduse",
        where_clause="Join_Count > 0"  # Dwel Units <=6
    )[0]

    joined_records_count = int(arcpy.GetCount_management(joined_feature)[0])
    print(f"\tNumber of Joined Records: {joined_records_count}. (Should be >100k)")

    if joined_records_count < 100000:
        logger.warning(f"\tNumber of Joined Records: {joined_records_count}. (Should be >100k)")
    else:
        logger.debug(f"\tNumber of Joined Records: {joined_records_count}. (Should be >100k)")

    return joined_feature


if __name__ == "__main__":

    # FEATURES
    BUILDING_POLYGONS_FEATURE = os.path.join(SDE, "SDEADM.BLD_building_polygon")
    WASTE_COLLECTION_AREAS_FEATURE = os.path.join(SDE, "SDEADM.ADM_solid_waste", "SDEADM.ADM_waste_coll_area")
    BUILDING_USE_TABLE = os.path.join(SDE, "SDEADM.BLD_BUILDING_USE")

    logger.info("Preparing Workspace...")
    
    # Create GEODATABASE and Use for as workspace
    working_gdb = arcpy.CreateFileGDB_management(
        out_folder_path=SCRIPT_DIR,
        out_name="temp_workspace.gdb"
    )[0]  # This will overwrite any existing gdb, clearing any existing feature classes

    # Local features
    local_bld_poly = arcpy.FeatureClassToFeatureClass_conversion(
        in_features=BUILDING_POLYGONS_FEATURE,
        out_path=working_gdb,
        out_name='BLD_building_polygon'
    )[0]
    local_bld_use = arcpy.TableToTable_conversion(
        in_rows=BUILDING_USE_TABLE,
        out_path=working_gdb,
        out_name='BLD_BUILDING_USE'
    )[0]
    local_waste_areas = arcpy.FeatureClassToFeatureClass_conversion(
        in_features=WASTE_COLLECTION_AREAS_FEATURE,
        out_path=working_gdb,
        out_name='ADM_waste_coll_area'
    )[0]

    try:
        waste_building_units = waste_analysis(
            workspace=working_gdb,
            building_polygons=local_bld_poly,
            building_use=local_bld_use,
            collection_areas=local_waste_areas
        )

        # SUMMARIZE ATTRIBUTES BY BL_ID, COLLECTION_AREA ON DWELLING UNITS
        print("\nSummarizing Attributes...")
        logger.debug("\nSummarizing Attributes...")

        df_final, df_summarized = summarize_units(dwellings_table=waste_building_units)

        summary_rows = len(df_summarized.index)
        area_1_units = df_final.loc["AREA 1", "SUM of DWELLING UNITS"]

        print(f"# of Summary Rows: {summary_rows}, Expected: ~130k")
        print(f"# of Area 1 Units: {area_1_units}, Expected: ~34k")

        if summary_rows < 130000:
            logger.warning(f"# of Summary Rows: {summary_rows}, Expected: ~130k")
        else:
            logger.debug(f"# of Summary Rows: {summary_rows}, Expected: ~130k")

        if area_1_units < 30000:
            logger.warning(f"# of Area 1 Units: {area_1_units}, Expected: ~34k")
        else:
            logger.debug(f"# of Area 1 Units: {area_1_units}, Expected: ~34k")

        # EXPORT TO EXCEL
        if len(df_summarized.index) > 0:
            print("\nExporting Report to Excel...")

            with pd.ExcelWriter(OUTPUT_EXCEL_NAME) as writer:
                df_final.to_excel(writer, sheet_name="final")
                df_summarized.to_excel(writer, sheet_name="summary")

            logger.debug("\nExported to Excel")

    except arcpy.ExecuteError:
        msg = f"ARCPY ERROR: {arcpy.GetMessages()}"

        logger.error(msg)
        print(msg)

    except Exception as e:
        print(e)
        logger.error(e)

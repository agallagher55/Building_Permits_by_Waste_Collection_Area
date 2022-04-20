"""
March 2022
Run-Time: ~20 minutes
A. Gallagher
Outputs: Excel file
- Important to make sure output excel file has 8 Collection Areas. Sometimes esri geoprocessing tool
gets stuck and needs to re-run. time.sleep() can help.
"""

import arcpy
import os
import logging

from time import sleep
from datetime import datetime
from configparser import ConfigParser

arcpy.env.overwriteOutput = True

TODAY = datetime.today().date().strftime('%m%d%Y')
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

OUTPUT_EXCEL_NAME = "Building_Dwellings_Summarized_{}.xlsx".format(TODAY)

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
LOG_FILE = "logs_{}.log".format(TODAY)

logger = logging.getLogger("__name__")
level = logging.DEBUG
logger.setLevel(level)

FORMATTER = logging.Formatter(
    '%(asctime)s | %(levelname)s | FUNCTION: %(funcName)s | Msgs: %(message)s', datefmt='%d-%b-%y %H:%M:%S'
)

handler = logging.FileHandler(LOG_FILE)
handler.setFormatter(FORMATTER)
logger.addHandler(handler)


def report(dwellings_table, working_gdb):

    output_table = os.path.join(working_gdb, "units_summary")

    # Update Table to where 'Coll_Area is None' to include dwellings outside a collection areain report
    print("\nUpdating {} with NO COLLECTION AREA zones...".format(dwellings_table))
    with arcpy.da.UpdateCursor(dwellings_table, "COLL_AREA", "COLL_AREA IS NULL") as cursor:
        for row in cursor:
            row[0] = "N/A"
            cursor.updateRow(row)

    # Get summary of dwelling units per collection area
    summary_fields = [["DWEL_UNITS", "SUM"]]
    case_fields = ["COLL_AREA", "BL_ID"]
    dwellings_summary = arcpy.Statistics_analysis(
        in_table=dwellings_table,
        out_table=output_table,
        statistics_fields=summary_fields,
        case_field=case_fields
    )[0]
    sleep(5)

    print("Filtering results...")
    dwel_units_sql = "SUM_DWEL_UNITS <= 6"
    filtered_dwellings, count = arcpy.SelectLayerByAttribute_management(
        in_layer_or_view=dwellings_summary,
        selection_type="NEW_SELECTION",
        where_clause=dwel_units_sql,
        invert_where_clause=""
    )
    sleep(5)

    if int(count) < 125000:
        logger.warning("# of records in filtered dwelling units table: {} (Expected ~131k)".format(count))
    else:
        logger.info("# of records in filtered dwelling units table: {} (Expected ~131k)".format(count))

    print("Exporting to excel...")
    if os.path.exists(OUTPUT_EXCEL_NAME):
        os.remove(OUTPUT_EXCEL_NAME)

    arcpy.TableToExcel_conversion(
        Input_Table=filtered_dwellings,
        Output_Excel_File=OUTPUT_EXCEL_NAME,
        Use_field_alias_as_column_header="ALIAS",
        Use_domain_and_subtype_description="CODE"
    )

    return OUTPUT_EXCEL_NAME


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

    # Export Joined Feature - may need for debug only
    arcpy.AddMessage("\tExporting Joined Features...")
    joined_feature = arcpy.FeatureClassToFeatureClass_conversion(
        in_features=building_polygons_w_bld_use,
        out_path=working_gdb,
        out_name="buildings_w_waste_areas_blduse",
        where_clause="Join_Count > 0"  # Dwel Units <=6
    )[0]

    joined_records_count = int(arcpy.GetCount_management(joined_feature)[0])
    arcpy.AddMessage("\tNumber of Joined Records: {}. (Should be >100k)".format(joined_records_count))

    if joined_records_count < 100000:
        logger.warning("Number of Joined Records: {}. (Should be >100k)".format(joined_records_count))
    else:
        logger.info("Number of Joined Records: {}. (Should be >100k)".format(joined_records_count))

    return working_gdb, joined_feature


if __name__ == "__main__":
    arcpy.env.overwriteOutput = True

    # working_gdb = os.path.join(SCRIPT_DIR, "temp_workspace.gdb")
    # dwelling_units = os.path.join(working_gdb, "buildings_w_waste_areas_blduse")

    try:
        working_gdb, dwelling_units = dwelling_units()
        excel_output = report(dwelling_units, working_gdb)

    except arcpy.ExecuteError:
        msg = "ARCPY ERROR: {}".format(arcpy.GetMessages())

        logger.error(msg)
        print(msg)

    except Exception as e:
        print(e)
        logger.error(e)

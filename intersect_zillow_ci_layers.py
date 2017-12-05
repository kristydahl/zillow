import arcpy
import glob
import os
import numpy
import csv
import json
import urllib, json
import pandas
from datetime import datetime
from arcpy.sa import *
arcpy.CheckOutExtension("Spatial")

path_to_state_csvs = 'C:/Users/kristydahl/Desktop/GIS_data/zillow/state_property_csvs/'
path_to_state_csvs_on_dropbox = 'C:/Users/kristydahl/Dropbox/zillow_data_on_dropbox/joined_data_csvs/'
arcpy.env.workspace = 'C:/Users/kristydahl/Desktop/GIS_data/zillow/zillow.gdb'
arcpy.env.overwriteOutput = True
spatialref = arcpy.SpatialReference(4326)
east_coast_spatialref = arcpy.SpatialReference(26917)
west_coast_spatialref = arcpy.SpatialReference(26910)

def csv_to_featureclass(state_number, region):
    print state_number
    print 'Creating XY layer'
    input_file = path_to_state_csvs_on_dropbox + 'state_{0}_joined_data.csv' .format(state_number)
    output_xyfile = 'state_{0}_joined_data' .format(state_number)
    output_file = 'state_{0}_joined_data_4326' .format(state_number)
    arcpy.MakeXYEventLayer_management(input_file, 'propertyaddresslongitude', 'propertyaddresslatitude', output_xyfile, spatialref)
    print 'Copying to featureclass'
    arcpy.CopyFeatures_management(output_xyfile, output_file)
    output_projected_file = 'state_{0}_joined_data_proj' .format(state_number)
    if region == 'east_coast':
        projected_spatialref = east_coast_spatialref
    else:
        projected_spatialref = west_coast_spatialref
    arcpy.Project_management(output_file, output_projected_file, projected_spatialref)
    print 'Projected featureclass'


def identify_properties_to_be_geocoded(state_number, region, year, projection):
    path_to_inundation_layers = 'C:/Users/kristydahl/Dropbox/UCS permanent inundation data/permanent_inundation/{0}/{0}.gdb/'.format(
        region)
    properties = 'state_{0}_joined_data_proj' .format(state_number)
    arcpy.MakeFeatureLayer_management(properties, 'properties_layer')

    state_number_inund = define_state_number_inund(state_number)
    print str(state_number_inund)

    if state_number == '1':
        state_number_inund = '01'
    if state_number == '6':
        state_number_inund = '06'

    # buffer the 2100 NCAH inundation layer by 0.05 mi
    print 'buffering layer start ' + str(datetime.now())
    if state_number == '42':
        layer_to_buffer = path_to_inundation_layers + 'final_polygon_extract_rg_merged_raw_raster_surface_26x_{0}_{1}_PA' .format(year, projection)
    elif state_number =='48':
        layer_to_buffer = path_to_inundation_layers + 'final_polygon_extract_rg_merged_raw_raster_surface_26x_2100_NCAH_gulf_to_tx_clip_to_48'
    else:
        layer_to_buffer = path_to_inundation_layers + 'final_polygon_26x_{0}_{1}_merged_clip_to_{2}' .format(year, projection, state_number_inund)
    outfile = 'final_polygon_{0}_{1}_state_{2}_005_buffer' .format(year, projection, state_number_inund)
    arcpy.Buffer_analysis(layer_to_buffer, outfile, "0.05 Miles")
    arcpy.MakeFeatureLayer_management(outfile, 'buffered_inundation_layer')
    print 'buffered layer end ' + str(datetime.now())

    # clip properties to the buffered inundation layer
    clip_output = 'state_{0}_joined_data_proj_clip_to_buffer' .format(state_number)
    print 'clipping start ' + str(datetime.now())
    arcpy.Clip_analysis('properties_layer', 'buffered_inundation_layer', clip_output)
    print 'clipping end ' + str(datetime.now())

    # select properties with null in geocoded column
    arcpy.MakeFeatureLayer_management(clip_output,'properties_layer')
    geocoded_null_query = " geocoded IS Null "
    print 'selecting geocode is null starting ' + str(datetime.now())
    arcpy.SelectLayerByAttribute_management('properties_layer',"NEW_SELECTION", geocoded_null_query)
    print 'selected geocode is null end ' + str(datetime.now())
    result = arcpy.GetCount_management('properties_layer')
    count = int(result.getOutput(0))
    print count
    outfile = 'state_{0}_joined_data_pre_geocode' .format(state_number)
    arcpy.CopyFeatures_management('properties_layer', outfile)

    # use cursor to loop through fc, writing row by row to a csv
    csv_filename = path_to_state_csvs + 'state_{0}_joined_data_to_geocode.csv' .format(state_number)
    print 'writing to csv start ' + str(datetime.now())
    with open(csv_filename, 'wb') as csvfile:
        fields = ['rowid','importparcelid','propertyfullstreetaddress','propertycity','propertystate','propertyzip'] #add loadid back in--not present for CA!
        writer = csv.DictWriter(csvfile, fieldnames=['rowid','importparcelid','loadid','address','city','state','zip_code'], delimiter=',')
        writer.writeheader()
        print 'Wrote header'

        with arcpy.da.UpdateCursor('properties_layer', fields) as cursor:
            for row in cursor:
                rowid = row[0]
                importparcelid = row[1]
                #loadid = row[2]
                propertyfullstreetaddress = row[2]
                propertycity = row[3]
                propertystate = row[4]
                propertyzip = row[5]
                if len(str(propertyzip)) == 4:
                    propertyzip = '0' + str(propertyzip)
                if propertyfullstreetaddress is not None:
                    writer = csv.writer(csvfile)
                    writer.writerow([rowid, importparcelid, propertyfullstreetaddress, propertycity, propertystate, propertyzip]) # add loadid back in--not present for CA!
        print 'Wrote to csv for sending to geocodio'

    #create new fc with properties that don't need to be geocoded
    geocoded_notnull_query = " geocoded = 'g' "
    arcpy.SelectLayerByAttribute_management('properties_layer',"NEW_SELECTION", geocoded_notnull_query)
    outfile = 'state_{0}_joined_data_already_geocoded' .format(state_number)
    arcpy.CopyFeatures_management('properties_layer', outfile)

def create_csv_from_pre_geocode_featureclass(state_number):
    fields = arcpy.ListFields('state_{0}_joined_data_pre_geocode' .format(state_number))
    field_names = [field.name for field in fields][3:]
    print field_names
    print len(field_names)

    outfile = path_to_state_csvs + 'state_{0}_joined_data_pre_geocode.csv' .format(state_number)
    with open(outfile,'wb') as csvfile:
        dw = csv.DictWriter(csvfile, field_names)
        dw.writeheader()
        with arcpy.da.UpdateCursor('state_{0}_joined_data_pre_geocode' .format(state_number), field_names) as cursor:
            for row in cursor:
                if str(row[33]) == 'None': # changed from 34 to 31 for ME and to 32 for NH and to 33 for FL
                    row[33] = 'zillow'
                    cursor.updateRow(row)
                dw.writerow(dict(zip(field_names, row)))
    print 'wrote to csv'

# join pre_geocode csv (which contains all fields) with geocodio_csv (which has limited fields but lat/long) to get
# a csv with all fields for all properties geocoded by geocodio
def join_geocodio_data_and_pre_geocode_data(state_number):
    pre_geocode_csv = path_to_state_csvs + 'state_{0}_joined_data_pre_geocode.csv' .format(state_number)
    pre_geocode_dataframe_all = pandas.read_table(pre_geocode_csv, header=0, sep=',',dtype='object',low_memory=False)
    pre_geocode_dataframe = pre_geocode_dataframe_all[['rowid','loadid','importparcelid','propertyfullstreetaddress',
                                                       'propertycity','propertystate','propertyzip','propertyhousenumber',
                                                       'taxamount','totalassessedvalue', 'assessmentyear',
                                                       'totalmarketvalue', 'marketvalueyear', 'totalappraisalvalue',
                                                       'appraisalvalueyear', 'noofunits','buildingconditionstndcode',
                                                       'foundationtypestndcode', 'totalbedrooms', 'propertylandusestndcode',
                                                       'yearbuilt', 'effectiveyearbuilt', 'buildingareasqft',
                                                       'buildingareastndcode', 'propertyaddressunitdesignator',
                                                       'propertyaddressunitnumber', 'loanratetypestndcode', 'loanduedate',
                                                       'propertysequencenumber', 'propertyaddresscensustractandblock',
                                                       'geocoded']] # 'propertyaddresscensustractandblock', 'created','updated' removed for ME, NY

    geocodio_csv = path_to_state_csvs + 'state_{0}_joined_data_geocoded_geocodio.csv' .format(state_number)
    geocodio_dataframe_all = pandas.read_table(geocodio_csv, header=0, sep=',',dtype='object')
    geocodio_dataframe = geocodio_dataframe_all[['importparcelid','Latitude','Longitude']]
    geocodio_dataframe.columns = ['importparcelid','propertyaddresslatitude','propertyaddresslongitude']

    join_all = pandas.merge(geocodio_dataframe, pre_geocode_dataframe, on='importparcelid')
    outfile = path_to_state_csvs + 'state_{0}_geocoded_geocodio_allfields.csv' .format(state_number)
    join_all.to_csv(outfile, sep=',',index=False)
    print 'output to csv'

# merge the properties geocoded by geocodio with those that were geocoded earlier to get a single feature class
def join_and_merge_for_final_properties_dataset(state_number, region):
    # create and project feature class of points geocoded by geocodio
    print 'Creating XY layer'
    input_file = path_to_state_csvs + 'state_{0}_geocoded_geocodio_allfields.csv'.format(state_number)
    output_xyfile = 'state_{0}_geocoded_geocodio_noproj'.format(state_number)
    output_file = 'state_{0}_geocoded_geocodio_4326'.format(state_number)
    arcpy.MakeXYEventLayer_management(input_file, 'propertyaddresslongitude', 'propertyaddresslatitude', output_xyfile, spatialref)
    print 'Copying to featureclass'
    arcpy.CopyFeatures_management(output_xyfile, output_file)
    output_projected_file = 'state_{0}_geocoded_geocodio_proj'.format(state_number)
    if region == 'east_coast':
        projected_spatialref = east_coast_spatialref
    else:
        projected_spatialref = west_coast_spatialref
    arcpy.Project_management(output_file, output_projected_file, projected_spatialref)
    print 'Projected featureclass'

    # merge with already_geocoded fc
    output_merged_properties = 'state_{0}_all_properties_geocoded' .format(state_number)
    arcpy.Merge_management([output_projected_file, 'state_{0}_joined_data_already_geocoded' .format(state_number)], output_merged_properties)
    print 'Merged'


# these methods should be run after the geocoding by geocodio
def erase_properties_within_levees(state_number):
    print 'Erasing leveed areas'
    input_file = 'state_{0}_all_properties_geocoded' .format(state_number)
    output_file = 'state_{0}_all_properties_geocoded_nonleveed' .format(state_number)
    leveed_areas = 'C:/Users/kristydahl/Desktop/GIS_data/usace_nld_leveed_area_coast_only.shp'
    arcpy.Erase_analysis(input_file, leveed_areas, output_file)

def define_state_number_inund(state_number):
    if state_number == '1':
        state_number_inund = '01'
    if state_number == '6':
        state_number_inund = '06'
    if state_number == '9':
        state_number_inund = '09'
    else:
        state_number_inund = state_number
    return state_number_inund

# ID CI properties for each state, year, and projection
def identify_ci_properties(state_number, region, years, projection):
    path_to_inundation_layers = 'C:/Users/kristydahl/Dropbox/UCS permanent inundation data/permanent_inundation/{0}/{0}.gdb/' .format(region)
    properties = 'state_{0}_all_properties_geocoded_nonleveed' .format(state_number)
    properties_layer = arcpy.MakeFeatureLayer_management(properties,'properties_layer')

    if state_number == '1':
        state_number_inund = '01'
    else:
        state_number_inund = define_state_number_inund(state_number)
    print 'state number inund is: ' + state_number_inund

    for year in years:
        if state_number == '42':
            inundated_area_file = path_to_inundation_layers + 'final_polygon_extract_rg_merged_raw_raster_surface_26x_{0}_{1}_PA'.format(year, projection)
        elif state_number == '48':
            inundated_area_file = path_to_inundation_layers + 'final_polygon_extract_rg_merged_raw_raster_surface_26x_{0}_{1}_gulf_to_tx_clip_to_48' .format(year, projection)
        else:
            inundated_area_file = path_to_inundation_layers + 'final_polygon_26x_{0}_{1}_merged_clip_to_{2}' .format(year, projection, state_number_inund)
        inundated_area_layer = arcpy.MakeFeatureLayer_management(inundated_area_file,'inundated_area_layer')
        outfile = 'ci_properties_state_{0}_{1}_{2}' .format(state_number, year, projection)
        print 'selecting properties within inundated area for year ' + year
        ci_properties = arcpy.SelectLayerByLocation_management('properties_layer', "WITHIN", 'inundated_area_layer')
        arcpy.FeatureClassToFeatureClass_conversion(ci_properties, arcpy.env.workspace,outfile)
        print 'saved'

def identify_ci_properties_fl(state_number, region, years, projection):
    path_to_inundation_layers = 'C:/Users/kristydahl/Dropbox/UCS permanent inundation data/permanent_inundation/{0}/{0}.gdb/' .format(region)
    properties = 'state_{0}_all_properties_geocoded_nonleveed' .format(state_number)
    properties_layer = arcpy.MakeFeatureLayer_management(properties,'properties_layer')

    for year in years:
        inundated_area_raster = path_to_inundation_layers + 'extract_26x_{0}_{1}_clip_to_{2}' .format(year, projection, state_number)
        print inundated_area_raster
        Xmin = str(arcpy.GetRasterProperties_management(inundated_area_raster, "LEFT").getOutput(0))
        Ymin = str(arcpy.GetRasterProperties_management(inundated_area_raster, "BOTTOM").getOutput(0))
        Xmax = str(arcpy.GetRasterProperties_management(inundated_area_raster, "RIGHT").getOutput(0))
        Ymax = str(arcpy.GetRasterProperties_management(inundated_area_raster, "TOP").getOutput(0))
        extents = '{0} {1} {2} {3}'.format(Xmin, Ymin, Xmax, Ymax)

        extents = '{0} {1} {2} {3}'.format(Xmin, Ymin, Xmax, Ymax)

        areas = ['1','2','3']
        for area in areas:
            area_polygon = path_to_inundation_layers + 'state_12_dissolved_{0}' .format(area)
            output_raster = path_to_inundation_layers + 'extract_26x_{0}_{1}_clip_to_{2}_area_{3}' .format(year, projection, state_number, area)
            arcpy.Clip_management(inundated_area_raster, "{0}".format(extents), output_raster, area_polygon, "#", "ClippingGeometry", "#")
            print 'Clipped inundated area raster to area'
            extract_to_convert = Con(Raster(output_raster) > 0, 1)
            inundated_area_fc = arcpy.RasterToPolygon_conversion(extract_to_convert,
                                                                             'state_12_inundation_surface_{0}_{1}_clip_to_area_{2}'.format(
                                                                                 year, projection, area))
            print 'converted raster to polygon'
            arcpy.RepairGeometry_management(inundated_area_fc)
            print 'repaired geometery of state inundation surface clip'

            arcpy.MakeFeatureLayer_management(inundated_area_fc,'inundated_area_layer')
            outfile = 'ci_properties_state_{0}_{1}_{2}_{3}' .format(state_number, year, projection, area)
            print 'selecting properties within inundated area for year ' + year + 'and area number ' + str(area)
            ci_properties = arcpy.SelectLayerByLocation_management('properties_layer', "WITHIN", 'inundated_area_layer')
            arcpy.FeatureClassToFeatureClass_conversion(ci_properties, arcpy.env.workspace,outfile)
            print 'saved'

        set_of_ci_properties = arcpy.ListFeatureClasses('ci_properties_state_{0}_{1}_{2}*' .format(state_number, year, projection))
        print set_of_ci_properties

        outfile = 'ci_properties_state_{0}_{1}_{2}' .format(state_number, year, projection)
        arcpy.Merge_management(set_of_ci_properties, outfile)

def add_fields_to_geography_layer(years, projection, geography_type): # geography type = states or cousub NEED TO ADD TOTAL AND AVE MARKET VALUE FIELDS
    if geography_type == 'states':
        file = 'states_for_testing_basic' # update this when appropriate
        arcpy.MakeFeatureLayer_management(file, 'layer')
    if geography_type == 'cousubs':
        file = 'coastal_county_subdivisions'
        arcpy.MakeFeatureLayer_management(file, 'layer')

    for year in years:
        out_feature_class = '{0}_for_prop_analysis_{1}_{2}' .format(geography_type, year, projection)
        arcpy.CopyFeatures_management('layer',out_feature_class)
        out_feature_class_layer = arcpy.MakeFeatureLayer_management(out_feature_class, 'out_feature_class_layer')
        arcpy.AddField_management('out_feature_class_layer', "CIprop","LONG")
        arcpy.AddField_management('out_feature_class_layer', "Totassval","FLOAT")
        arcpy.AddField_management('out_feature_class_layer', "Totmarkval","FLOAT")
        arcpy.AddField_management('out_feature_class_layer', "Tottax", "FLOAT")
        arcpy.AddField_management('out_feature_class_layer', "Aveassval","FLOAT")
        arcpy.AddField_management('out_feature_class_layer', "Avemarkval","FLOAT")
        arcpy.AddField_management('out_feature_class_layer', "Aveyearbuilt","LONG")
        arcpy.AddField_management('out_feature_class_layer', "Loanduenotnull","LONG")
        #arcpy.AddField_management('out_feature_class_layer', "Loanduebyyear","LONG")
        arcpy.AddField_management('out_feature_class_layer', "Totbeds","LONG")
        arcpy.AddField_management('out_feature_class_layer', "Totsqft","LONG")
        arcpy.AddField_management('out_feature_class_layer', "RIprop","LONG")
        arcpy.AddField_management('out_feature_class_layer', "TotEXGDcond","LONG")
        arcpy.AddField_management('out_feature_class_layer', "TotAVcond","LONG")
        arcpy.AddField_management('out_feature_class_layer', "TotFRPRUNcond","LONG")
        arcpy.AddField_management('out_feature_class_layer', "Totsafefdn", "LONG")
        # add more lines here for other fields

def define_fc_fields_for_geography_type(geography_type):
    if geography_type == 'state':
        geography_fields = ["CIprop",'Totassval', 'Totmarkval','Tottax','Aveassval', 'Avemarkval','Aveyearbuilt','Loanduenotnull','Totbeds','Totsqft','RIprop',
                          'TotEXGDcond','TotAVcond','TotFRPRUNcond','Totsafefdn', "SHAPE@"]
    if geography_type == 'cousub':
        geography_fields = ["CIprop", 'Totassval', 'Totmarkval','Tottax', 'Aveassval', 'Avemarkval','Aveyearbuilt', 'Loanduenotnull', 'Totbeds', 'Totsqft', 'RIprop',
                  'TotEXGDcond', 'TotAVcond', 'TotFRPRUNcond', 'Totsafefdn', "ACS_2014_5YR_COUSUB_COUNTYFP",
                  "ACS_2014_5YR_COUSUB_NAME", "SHAPE@"]

    return(geography_fields)

def define_csv_fields_to_write_by_geography_type(geography_type):
    if geography_type == 'state':
        fieldnames = ['Year', 'Projection', 'State Number', 'Number CI Properties', 'Total Assessed Value',
                      'Total Market Value', 'Total Property Tax', 'Average Assessed Value', 'Average Market Value',
                      'Average Year Built', 'Total Bedrooms', 'Total Square Footage', 'Number with Loan Due Info',
                      'Number of Rentals', 'Condition Excellent or Good', 'Condition Average',
                      'Condition Fair Poor or Unsound', 'Number with Safe Foundations']
    if geography_type == 'cousub':
        fieldnames == ['Year', 'Projection', 'State Number', 'County Code', 'Name','Number CI Properties', 'Total Assessed Value',
                      'Total Market Value', 'Total Property Tax', 'Average Assessed Value', 'Average Market Value',
                      'Average Year Built', 'Total Bedrooms', 'Total Square Footage', 'Number with Loan Due Info',
                      'Number of Rentals', 'Condition Excellent or Good', 'Condition Average',
                      'Condition Fair Poor or Unsound', 'Number with Safe Foundations']

    return(fieldnames)

def select_residential_properties_and_save_as_fc(state_number, year, projection):
    ci_properties = 'ci_properties_state_{0}_{1}_{2}'.format(state_number, year, projection)
    arcpy.MakeFeatureLayer_management(ci_properties, 'ci_properties')
    select_residential_query = " propertylandusestndcode LIKE 'RR%' OR propertylandusestndcode LIKE 'RI%' ".format(
        state_number)
    arcpy.SelectLayerByAttribute_management('ci_properties', "NEW_SELECTION", select_residential_query)
    outfile = 'res_ci_properties_state_{0}_{1}_{2}' .format(state_number, year, projection)
    fc = arcpy.CopyFeatures_management('ci_properties', outfile)
    return(outfile)


def output_statistics_by_geography_type(state_number, geography_type, years, projection):
    # define csv file name, fields, etc.
    state_number_inund = define_state_number_inund(state_number)
    csv_filename = 'C:/Users/kristydahl/Desktop/GIS_data/zillow/{0}_statistics/state_{1}_ci_property_stats_{2}.csv'.format(
        geography_type, state_number_inund, projection)

    # get fieldnames for writing to csv
    fieldnames = define_csv_fields_to_write_by_geography_type(geography_type)
    with open(csv_filename, 'wb') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
        writer.writeheader()
        print 'Wrote header'

        for year in years:
            print 'Year is ' + year
            fc = select_residential_properties_and_save_as_fc(state_number, year, projection)
            fc = arcpy.MakeFeatureLayer_management(fc, 'fc')
            geography_file = '{0}s_for_prop_analysis_{1}_{2}'.format(geography_type, year, projection)  # update this when appropriate
            geography_layer = arcpy.MakeFeatureLayer_management(geography_file, 'geography_layer')

            # select only state of interest
            if geography_type == 'state':
                select_query = " STATE_FIPS = '{0}' ".format(state_number_inund) # this will yield one row
            if geography_type == 'cousub':
                select_query = " ACS_2014_5YR_COUSUB_STATEFP = '{0}' ".format(state_number_inund) # this will yield a row for each cousub in state
            arcpy.SelectLayerByAttribute_management(geography_layer, "NEW_SELECTION", select_query)

            geography_fields = define_fc_fields_for_geography_type(geography_type)
            print geography_fields
            index_of_shape_field = len(geography_fields) - 1

            with arcpy.da.UpdateCursor('geography_layer', geography_fields) as cursor:
                for row in cursor:
                    print row
                    shape_field = row[index_of_shape_field]
                    # select properties within geography type if type is other than states
                    if geography_type != 'states':
                        arcpy.SelectLayerByLocation_management(fc, "WITHIN", shape_field)

                    # get stats on geographic unit
                    number_of_properties = int(arcpy.GetCount_management(fc).getOutput(0))

                    # total and average assessed value
                    arcpy.Statistics_analysis(fc, 'output_table_total_ass_val', [["totalassessedvalue", "SUM"]])
                    total_ass_val = arcpy.da.TableToNumPyArray('output_table_total_ass_val', 'SUM_totalassessedvalue')[0][0]

                    arcpy.Statistics_analysis(fc, 'output_table_ave_ass_val', [["totalassessedvalue", "MEAN"]])
                    ave_ass_val = arcpy.da.TableToNumPyArray('output_table_ave_ass_val', 'MEAN_totalassessedvalue')[0][0]

                    print 'total ass value is ' + str(total_ass_val)

                    # total and average market value
                    arcpy.Statistics_analysis(fc, 'count_total_mark_val', [["totalmarketvalue", "COUNT"]])
                    number_not_null = arcpy.da.TableToNumPyArray('count_total_mark_val', 'COUNT_total_market_value')[0][0]

                    if number_not_null != 0:
                        arcpy.Statistics_analysis(fc, 'output_table_total_mark_val', [["totalmarketvalue", "SUM"]])
                        total_mark_val_with_nulls = arcpy.da.TableToNumPyArray('output_table_total_mark_val','SUM_totalmarketvalue')[0][0]
                        arcpy.da.TableToNumPyArray('output_table_total_mark_val', 'SUM_totalmarketvalue')[0][0]

                        arcpy.Statistics_analysis(fc, 'output_table_ave_mark_val', [["totalmarketvalue", "MEAN"]])
                        ave_mark_val = arcpy.da.TableToNumPyArray('output_table_ave_mark_val', 'MEAN_totalmarketvalue')[0][0]

                        arcpy.Statistics_analysis(fc, 'output_total_mark_val_null', [["totalmarketvalue", "COUNT"]])
                        number_nulls = arcpy.da.TableToNumPyArray('output_total_mark_val_null', 'COUNT_totalmarketvalue')[0][0]

                    # calculate total market value using average and number of nulls
                    arcpy.SelectLayerByAttribute_management
                    total_mark_val = total_mark_val_with_nulls + ave_mark_val * number_nulls

                    print 'total market value is ' + str(total_mark_val)

                    # total property taxes
                    arcpy.Statistics_analysis(fc, 'output_table_total_tax', [["taxamount", "SUM"]])
                    total_tax = arcpy.da.TableToNumPyArray('output_table_total_tax', 'SUM_taxamount')[0][0]

                    # average year built
                    arcpy.Statistics_analysis(fc, 'output_table_ave_yrbuilt', [["yearbuilt", "MEAN"]])  # changed to 2 for LA--need to change back
                    ave_yrbuilt = arcpy.da.TableToNumPyArray('output_table_ave_yrbuilt', ["MEAN_yearbuilt"])[0][0]

                    # total bedrooms
                    arcpy.Statistics_analysis(fc, 'output_table_total_beds', [["totalbedrooms", "SUM"]])
                    total_bedrooms = arcpy.da.TableToNumPyArray('output_table_total_beds', 'SUM_totalbedrooms')[0][0]

                    # total square footage

                    arcpy.Statistics_analysis(fc, 'output_table_total_sqft',
                                              [["buildingareasqft", "SUM"]])  # changed for NC--need to change back!
                    total_sqft = arcpy.da.TableToNumPyArray('output_table_total_sqft', 'SUM_buildingareasqft')[0][0]

                    # number of properties with not null loanduedate
                    select_notnull_query = " loanduedate IS NOT Null".format(state_number)
                    arcpy.SelectLayerByAttribute_management('ci_properties', "SUBSET_SELECTION", select_notnull_query)
                    fc = arcpy.MakeFeatureLayer_management('ci_properties', 'notnull_ci_properties')
                    loanduedate_notnull = int(arcpy.GetCount_management(fc).getOutput(0))

                    # get subset that are RI properties
                    fc = select_residential_properties_and_save_as_fc(state_number, year, projection)
                    fc = arcpy.MakeFeatureLayer_management(fc, 'fc')
                    select_RI_query = " propertylandusestndcode LIKE 'RI%' ".format(state_number)
                    arcpy.SelectLayerByAttribute_management('fc', "NEW_SELECTION", select_RI_query)
                    fc = arcpy.MakeFeatureLayer_management('fc', 'ri_ci_properties')
                    number_of_RI_properties = int(arcpy.GetCount_management(fc).getOutput(0))

                    # get subsets of properties with various building conditions
                    fc = select_residential_properties_and_save_as_fc(state_number, year, projection)
                    fc = arcpy.MakeFeatureLayer_management(fc, 'fc')
                    select_EXGD_query = " buildingconditionstndcode IN('EX','GD') ".format(state_number)
                    arcpy.SelectLayerByAttribute_management('fc', "SUBSET_SELECTION", select_EXGD_query)
                    fc = arcpy.MakeFeatureLayer_management('fc', 'exgd_ci_properties')
                    number_of_EXGD_properties = int(arcpy.GetCount_management(fc).getOutput(0))

                    fc = select_residential_properties_and_save_as_fc(state_number, year, projection)
                    fc = arcpy.MakeFeatureLayer_management(fc, 'fc')
                    select_AV_query = " buildingconditionstndcode IN('AV')  ".format(state_number)
                    arcpy.SelectLayerByAttribute_management('fc', "SUBSET_SELECTION", select_AV_query)
                    fc = arcpy.MakeFeatureLayer_management('fc', 'av_ci_properties')
                    number_of_AV_properties = int(arcpy.GetCount_management(fc).getOutput(0))

                    fc = select_residential_properties_and_save_as_fc(state_number, year, projection)
                    fc = arcpy.MakeFeatureLayer_management(fc, 'fc')
                    select_FRPRUN_query = " buildingconditionstndcode IN('FR','PR','UN') ".format(state_number)
                    arcpy.SelectLayerByAttribute_management('fc', "SUBSET_SELECTION", select_FRPRUN_query)
                    fc = arcpy.MakeFeatureLayer_management('fc', 'frprun_ci_properties')
                    number_of_FRPRUN_properties = int(arcpy.GetCount_management(fc).getOutput(0))

                    # get subset of properties with "flood safe" foundation types
                    fc = select_residential_properties_and_save_as_fc(state_number, year, projection)
                    fc = arcpy.MakeFeatureLayer_management(fc, 'fc')
                    select_safe_fdn_query = " foundationtypestndcode IN('CS','RF','PI','PD') ".format(state_number)
                    arcpy.SelectLayerByAttribute_management('ci_properties', "SUBSET_SELECTION", select_safe_fdn_query)
                    fc = arcpy.MakeFeatureLayer_management('fc', 'safe_fdn_ci_properties')
                    number_of_safe_fdn_properties = int(arcpy.GetCount_management(fc).getOutput(0))

                    # update feature class row
                    row[0] = number_of_properties
                    row[1] = total_ass_val
                    row[2] = total_mark_val
                    row[3] = total_tax
                    row[3] = ave_ass_val
                    row[4] = ave_mark_val
                    row[5] = ave_yrbuilt
                    row[6] = loanduedate_notnull
                    row[7] = total_bedrooms
                    row[8] = total_sqft
                    row[9] = number_of_RI_properties
                    row[10] = number_of_EXGD_properties
                    row[11] = number_of_AV_properties
                    row[12] = number_of_FRPRUN_properties
                    row[13] = number_of_safe_fdn_properties
                    cursor.updateRow(row)

                    # write to csv

                    if geography_type == 'state':
                        writer.writerow(
                            [year, projection, state_number_inund, number_of_properties, "%.2f" % total_ass_val,
                             "%.2f" % total_mark_val, "%.2f" % total_tax,
                             "%.2f" % ave_ass_val, "%.2f" % ave_mark_val, ave_yrbuilt, total_bedrooms, "%.2f" % total_sqft,
                             loanduedate_notnull, number_of_RI_properties,
                             number_of_EXGD_properties, number_of_AV_properties, number_of_FRPRUN_properties,
                             number_of_safe_fdn_properties])
                        print 'Wrote to csv'

                    if geography_type == 'cousub':
                        county_code = row[13]
                        cousub_name = row[14]
                        writer.writerow([year, projection, state_number_inund, county_code, cousub_name, number_of_properties, "%.2f" % total_ass_val,
                         "%.2f" % total_mark_val, "%.2f" % total_tax,
                         "%.2f" % ave_ass_val, "%.2f" % ave_mark_val, ave_yrbuilt, total_bedrooms, "%.2f" % total_sqft,
                         loanduedate_notnull, number_of_RI_properties,
                         number_of_EXGD_properties, number_of_AV_properties, number_of_FRPRUN_properties,
                         number_of_safe_fdn_properties])
                        print 'Wrote to csv'









# # Output state statistics for year, projection, number of CI properties, total value of CI properties
# def output_state_statistics(state_number, years, projection):
#     state_number_inund = define_state_number_inund(state_number)
#     csv_filename = 'C:/Users/kristydahl/Desktop/GIS_data/zillow/state_statistics/state_{0}_ci_property_stats_{1}.csv'.format(
#         state_number_inund, projection)
#
#     with open(csv_filename, 'wb') as csvfile:
#         writer = csv.DictWriter(csvfile, fieldnames=['Year', 'Projection', 'State Number', 'Number CI Properties', 'Total Assessed Value', 'Total Market Value', 'Total Property Tax', 'Average Assessed Value', 'Average Market Value','Average Year Built', 'Total Bedrooms', 'Total Square Footage', 'Number with Loan Due Info', 'Number of Rentals', 'Condition Excellent or Good', 'Condition Average', 'Condition Fair Poor or Unsound', 'Number with Safe Foundations'], delimiter=',')
#         writer.writeheader()
#         print 'Wrote header'
#
#         writer = csv.writer(csvfile)
#         for year in years:
#             print 'Year is ' + year
#             states_file = 'states_for_prop_analysis_{0}_{1}' .format(year, projection)# update this when appropriate
#             states_layer = arcpy.MakeFeatureLayer_management(states_file,'states_layer')
#             select_query = " STATE_FIPS = '{0}' " .format(state_number_inund)
#             arcpy.SelectLayerByAttribute_management(states_layer, "NEW_SELECTION", select_query)
#
#             ci_properties = 'ci_properties_state_{0}_{1}_{2}' .format(state_number, year, projection)
#             arcpy.MakeFeatureLayer_management(ci_properties,'ci_properties')
#
#             #### THIS IS ALL THE STUFF THAT SHOULD GET PULLED OUT INTO A SEPARATE METHOD FOR CALLING WITHIN STATE, COUSUB, OR CD METHODS
#             #select only residential properties
#             select_residential_query = " propertylandusestndcode LIKE 'RR%' OR propertylandusestndcode LIKE 'RI%' " .format(state_number)
#             arcpy.SelectLayerByAttribute_management('ci_properties', "NEW_SELECTION", select_residential_query)
#
#             #get stats on residential properties
#             # number of CI properties
#             fc = arcpy.MakeFeatureLayer_management('ci_properties', 'res_ci_properties')
#             number_of_properties = int(arcpy.GetCount_management(fc).getOutput(0))
#
#             if number_of_properties > 0:
#
#                 # total and average assessed value
#                 arcpy.Statistics_analysis(fc, 'output_table_total_ass_val', [["totalassessedvalue", "SUM"]])
#                 total_ass_val = arcpy.da.TableToNumPyArray('output_table_total_ass_val','SUM_totalassessedvalue')[0][0]
#
#                 arcpy.Statistics_analysis(fc, 'output_table_ave_ass_val', [["totalassessedvalue", "MEAN"]])
#                 ave_ass_val = arcpy.da.TableToNumPyArray('output_table_ave_ass_val', 'MEAN_totalassessedvalue')[0][0]
#
#                 print 'total ass value is ' + str(total_ass_val)
#
#                 # total and average market value
#                 arcpy.Statistics_analysis(fc, 'output_table_total_mark_val', [["totalmarketvalue", "SUM"]])
#                 total_mark_val_with_nulls = arcpy.da.TableToNumPyArray('output_table_total_mark_val','SUM_totalmarketvalue')[0][0]
#
#                 arcpy.Statistics_analysis(fc, 'output_table_ave_mark_val', [["totalmarketvalue", "MEAN"]])
#                 ave_mark_val = arcpy.da.TableToNumPyArray('output_table_ave_mark_val','MEAN_totalmarketvalue')[0][0]
#
#                 arcpy.Statistics_analysis(fc, 'output_total_mark_val_null', [["totalmarketvalue", "COUNT"]])
#                 number_nulls = arcpy.da.TableToNumPyArray('output_total_mark_val_null', 'COUNT_totalmarketvalue')[0][0]
#
#                 # calculate total market value using average and number of nulls
#                 arcpy.SelectLayerByAttribute_management
#                 total_mark_val = total_mark_val_with_nulls + ave_mark_val * number_nulls
#
#                 print 'total market value is ' + str(total_mark_val)
#
#                 # total property taxes
#                 arcpy.Statistics_analysis(fc, 'output_table_total_tax', [["taxamount", "SUM"]])
#                 total_tax = arcpy.da.TableToNumPyArray('output_table_total_tax','SUM_taxamount')[0][0]
#
#                 # average year built
#                 arcpy.Statistics_analysis(fc, 'output_table_ave_yrbuilt', [["yearbuilt", "MEAN"]]) # changed to 2 for LA--need to change back
#                 ave_yrbuilt = arcpy.da.TableToNumPyArray('output_table_ave_yrbuilt',["MEAN_yearbuilt"])[0][0]
#
#                 # total bedrooms
#                 arcpy.Statistics_analysis(fc, 'output_table_total_beds', [["totalbedrooms", "SUM"]])
#                 total_bedrooms = arcpy.da.TableToNumPyArray('output_table_total_beds','SUM_totalbedrooms')[0][0]
#
#                 # total square footage
#
#                 arcpy.Statistics_analysis(fc, 'output_table_total_sqft', [["buildingareasqft", "SUM"]]) # changed for NC--need to change back!
#                 total_sqft = arcpy.da.TableToNumPyArray('output_table_total_sqft','SUM_buildingareasqft')[0][0]
#
#                 # number of properties with not null loanduedate
#                 select_notnull_query = " loanduedate IS NOT Null" .format(state_number)
#                 arcpy.SelectLayerByAttribute_management('ci_properties',"SUBSET_SELECTION", select_notnull_query)
#                 fc = arcpy.MakeFeatureLayer_management('ci_properties', 'notnull_ci_properties')
#                 loanduedate_notnull = int(arcpy.GetCount_management(fc).getOutput(0))
#
#                 # get subset that are RI properties
#                 select_RI_query = " propertylandusestndcode LIKE 'RI%' " .format(state_number)
#                 arcpy.SelectLayerByAttribute_management('ci_properties',"NEW_SELECTION", select_RI_query)
#                 fc = arcpy.MakeFeatureLayer_management('ci_properties', 'ri_ci_properties')
#                 number_of_RI_properties = int(arcpy.GetCount_management(fc).getOutput(0))
#
#                 # get subsets of properties with various building conditions
#                 arcpy.SelectLayerByAttribute_management('ci_properties', "NEW_SELECTION", select_residential_query)
#                 select_EXGD_query = " buildingconditionstndcode IN('EX','GD') " .format(state_number)
#                 arcpy.SelectLayerByAttribute_management('ci_properties',"SUBSET_SELECTION", select_EXGD_query)
#                 fc = arcpy.MakeFeatureLayer_management('ci_properties', 'exgd_ci_properties')
#                 number_of_EXGD_properties = int(arcpy.GetCount_management(fc).getOutput(0))
#                 arcpy.SelectLayerByAttribute_management('ci_properties', "NEW_SELECTION", select_residential_query)
#                 select_AV_query = " buildingconditionstndcode IN('AV')  " .format(state_number)
#                 arcpy.SelectLayerByAttribute_management('ci_properties',"SUBSET_SELECTION", select_AV_query)
#                 fc = arcpy.MakeFeatureLayer_management('ci_properties', 'av_ci_properties')
#                 number_of_AV_properties = int(arcpy.GetCount_management(fc).getOutput(0))
#                 arcpy.SelectLayerByAttribute_management('ci_properties', "NEW_SELECTION", select_residential_query)
#                 select_FRPRUN_query = " buildingconditionstndcode IN('FR','PR','UN') " .format(state_number)
#                 arcpy.SelectLayerByAttribute_management('ci_properties',"SUBSET_SELECTION", select_FRPRUN_query)
#                 fc = arcpy.MakeFeatureLayer_management('ci_properties', 'frprun_ci_properties')
#                 number_of_FRPRUN_properties = int(arcpy.GetCount_management(fc).getOutput(0))
#
#                 # get subset of properties with "flood safe" foundation types
#                 arcpy.SelectLayerByAttribute_management('ci_properties', "NEW_SELECTION", select_residential_query)
#                 select_safe_fdn_query = " foundationtypestndcode IN('CS','RF','PI','PD') " .format(state_number)
#                 arcpy.SelectLayerByAttribute_management('ci_properties', "SUBSET_SELECTION", select_safe_fdn_query)
#                 fc = arcpy.MakeFeatureLayer_management('ci_properties', 'safe_fdn_ci_properties')
#                 number_of_safe_fdn_properties = int(arcpy.GetCount_management(fc).getOutput(0))
#
#                 # write to csv
#                 writer.writerow(
#                     [year, projection, state_number_inund, number_of_properties, "%.2f" % total_ass_val, "%.2f" % total_mark_val, "%.2f" % total_tax,
#                      "%.2f" % ave_ass_val, "%.2f" % ave_mark_val, ave_yrbuilt, total_bedrooms, "%.2f" % total_sqft, loanduedate_notnull, number_of_RI_properties,
#                      number_of_EXGD_properties, number_of_AV_properties, number_of_FRPRUN_properties, number_of_safe_fdn_properties])
#                 print 'Wrote to csv'
#
#                 fields = ["CIprop",'Totassval', 'Totmarkval','Tottax','Aveassval', 'Avemarkval','Aveyearbuilt','Loanduenotnull','Totbeds','Totsqft','RIprop',
#                           'TotEXGDcond','TotAVcond','TotFRPRUNcond','Totsafefdn']
#                 # write stats to state feature class
#                 with arcpy.da.UpdateCursor('states_layer', fields) as cursor:
#                     for row in cursor:
#                         row[0] = number_of_properties
#                         row[1] = total_ass_val
#                         row[2] = total_mark_val
#                         row[3] = total_tax
#                         row[3] = ave_ass_val
#                         row[4] = ave_mark_val
#                         row[5] = ave_yrbuilt
#                         row[6] = loanduedate_notnull
#                         row[7] = total_bedrooms
#                         row[8] = total_sqft
#                         row[9] = number_of_RI_properties
#                         row[10] = number_of_EXGD_properties
#                         row[11] = number_of_AV_properties
#                         row[12] = number_of_FRPRUN_properties
#                         row[13] = number_of_safe_fdn_properties
#
#                         cursor.updateRow(row)
#
# # Aggregate and Output year, projection, number of CI properties, total value of CI properties per county subdivision; NEED TO UPDATE WITH CHANGES MADE TO ABOVE STATE METHOD
# def output_county_subdivision_statistics(state_number, years, projection):
#     state_number_inund = define_state_number_inund(state_number)
#     csv_filename = 'C:/Users/kristydahl/Desktop/GIS_data/zillow/county_subdivision_statistics/state_{0}_cousub__ci_property_stats.csv'.format(state_number_inund)
#
#     with open(csv_filename, 'wb') as csvfile:
#         writer = csv.writer(csvfile)
#         for year in years:
#             cousubs_file = 'cousubs_for_prop_analysis_{0}_{1}' .format(year, projection)
#             arcpy.MakeFeatureLayer_management(cousubs_file, 'cousubs_layer')
#
#             # select only the county subdivisions within the state_number
#             select_state_query = " ACS_2014_5YR_COUSUB_STATEFP = '{0}' " .format(state_number_inund)
#             arcpy.SelectLayerByAttribute_management('cousubs_layer', "NEW_SELECTION", select_state_query)
#
#             # load up the ci_properties data and just get the residential properties
#             ci_properties = 'ci_properties_state_{0}_{1}_{2}' .format(state_number, year, projection)
#             arcpy.MakeFeatureLayer_management(ci_properties,'ci_properties')
#             select_residential_query = " propertylandusestndcode LIKE 'RR%' OR propertylandusestndcode LIKE 'RI%' "
#             arcpy.SelectLayerByAttribute_management('ci_properties', "NEW_SELECTION", select_residential_query)
#             res_ci_properties = arcpy.MakeFeatureLayer_management('ci_properties', 'res_ci_properties')
#
#             # get stats on residential properties
#             # number of CI properties
#             fc = arcpy.MakeFeatureLayer_management('ci_properties', 'res_ci_properties')
#             number_of_properties = int(arcpy.GetCount_management(fc).getOutput(0))
#             # total and average assessed value
#             arcpy.Statistics_analysis(fc, 'output_table_total_ass_val', [["totalassessedvalue", "SUM"]])
#             total_ass_val = arcpy.da.TableToNumPyArray('output_table_total_ass_val', 'SUM_totalassessedvalue')[0][0]
#             arcpy.Statistics_analysis(fc, 'output_table_ave_ass_val', [["totalassessedvalue", "MEAN"]])
#             ave_ass_val = arcpy.da.TableToNumPyArray('output_table_ave_ass_val', 'MEAN_totalassessedvalue')[0][0]
#             # total property taxes
#             arcpy.Statistics_analysis(fc, 'output_table_total_tax', [["taxamount", "SUM"]])
#             total_tax = arcpy.da.TableToNumPyArray('output_table_total_tax', 'SUM_taxamount')[0][0]
#             # average year built
#             arcpy.Statistics_analysis(fc, 'output_table_ave_yrbuilt', [["yearbuilt", "MEAN"]])
#             ave_yrbuilt = arcpy.da.TableToNumPyArray('output_table_ave_yrbuilt', 'MEAN_yearbuilt')[0][0]
#             # total bedrooms
#             arcpy.Statistics_analysis(fc, 'output_table_total_beds', [["totalbedrooms", "SUM"]])
#             total_bedrooms = arcpy.da.TableToNumPyArray('output_table_total_beds', 'SUM_totalbedrooms')[0][0]
#             # total squre footage
#             arcpy.Statistics_analysis(fc, 'output_table_total_sqft', [["buildingareasqft", "SUM"]])
#             total_sqft = arcpy.da.TableToNumPyArray('output_table_total_sqft', 'SUM_buildingareasqft')[0][0]
#             # number of properties with not null loanduedate
#             select_notnull_query = " propertylandusestndcode LIKE 'RI%' OR propertylandusestndcode LIKE 'RR%' AND loanduedate IS NOT Null "
#             arcpy.SelectLayerByAttribute_management('ci_properties',"NEW_SELECTION", select_notnull_query)
#             fc = arcpy.MakeFeatureLayer_management('ci_properties', 'notnull_ci_properties')
#             loanduedate_notnull = int(arcpy.GetCount_management(fc).getOutput(0))
#
#             # get subset that are RI properties
#             select_RI_query = " propertylandusestndcode LIKE 'RI%' "
#             arcpy.SelectLayerByAttribute_management('ci_properties', "NEW_SELECTION", select_RI_query)
#             fc = arcpy.MakeFeatureLayer_management('ci_properties', 'ri_ci_properties')
#             number_of_RI_properties = int(arcpy.GetCount_management(fc).getOutput(0))
#
#             # get subsets of properties with various building conditions
#             arcpy.SelectLayerByAttribute_management('ci_properties', "NEW_SELECTION", select_residential_query)
#             select_EXGD_query = " buildingconditionstndcode IN('EX','GD') "
#             arcpy.SelectLayerByAttribute_management('ci_properties',"SUBSET_SELECTION", select_EXGD_query)
#             fc = arcpy.MakeFeatureLayer_management('ci_properties', 'exgd_ci_properties')
#             number_of_EXGD_properties = int(arcpy.GetCount_management(fc).getOutput(0))
#             arcpy.SelectLayerByAttribute_management('ci_properties', "NEW_SELECTION", select_residential_query)
#             select_AV_query = " buildingconditionstndcode IN('AV') "
#             arcpy.SelectLayerByAttribute_management('ci_properties',"SUBSET_SELECTION", select_AV_query)
#             fc = arcpy.MakeFeatureLayer_management('ci_properties', 'av_ci_properties')
#             number_of_AV_properties = int(arcpy.GetCount_management(fc).getOutput(0))
#             arcpy.SelectLayerByAttribute_management('ci_properties', "NEW_SELECTION", select_residential_query)
#             select_FRPRUN_query = " buildingconditionstndcode IN('FR','PR','UN') "
#             arcpy.SelectLayerByAttribute_management('ci_properties',"SUBSET_SELECTION", select_FRPRUN_query)
#             fc = arcpy.MakeFeatureLayer_management('ci_properties', 'frprun_ci_properties')
#             number_of_FRPRUN_properties = int(arcpy.GetCount_management(fc).getOutput(0))
#
#             # get subset of properties with "flood safe" foundation types
#             arcpy.SelectLayerByAttribute_management('ci_properties', "NEW_SELECTION", select_residential_query)
#             select_safe_fdn_query = " foundationtypestndcode IN('CS','RF','PI','PD') "
#             arcpy.SelectLayerByAttribute_management('ci_properties', "SUBSET_SELECTION", select_safe_fdn_query)
#             fc = arcpy.MakeFeatureLayer_management('ci_properties', 'safe_fdn_ci_properties')
#             number_of_safe_fdn_properties = int(arcpy.GetCount_management(fc).getOutput(0))
#
#             print number_of_properties
#             print total_ass_val
#             print total_tax
#
#             fields = ["CIprop",'Totassval','Tottax','Aveassval','Aveyearbuilt','Loanduenotnull','Totbeds','Totsqft','RIprop',
#                       'TotEXGDcond','TotAVcond','TotFRPRUNcond','Totsafefdn',"ACS_2014_5YR_COUSUB_COUNTYFP","ACS_2014_5YR_COUSUB_NAME", "SHAPE@"]
#
#             # write stats to cousub feature class
#             with arcpy.da.UpdateCursor('cousubs_layer', fields) as cursor:
#                 for row in cursor:
#                     print row
#                     cousub = row[15]
#                     print cousub
#                     # select properties within cousub
#                     arcpy.SelectLayerByLocation_management(res_ci_properties, "WITHIN", cousub)
#
#                     # get stats and update fields in row
#                     number_of_properties = int(arcpy.GetCount_management(res_ci_properties).getOutput(0))
#                     arcpy.Statistics_analysis(res_ci_properties, 'output_table_total_ass_val',
#                                               [["totalassessedvalue", "SUM"]])
#                     total_ass_val = arcpy.da.TableToNumPyArray('output_table_total_ass_val', 'SUM_totalassessedvalue')[0][0] # error here "index 0 is out of bounds for axis 0..."
#                     arcpy.Statistics_analysis(res_ci_properties, 'output_table_total_tax',
#                                               [["taxamount", "SUM"]])
#                     total_tax = arcpy.da.TableToNumPyArray('output_table_total_tax', 'SUM_taxamount')[0][0]
#                     print number_of_properties
#                     print total_ass_val
#                     print total_tax
#
#                     # update row
#                     row[0] = number_of_properties
#                     row[1] = total_ass_val
#                     row[2] = total_tax
#                     row[3] = ave_ass_val
#                     row[4] = ave_yrbuilt
#                     row[5] = loanduedate_notnull
#                     row[6] = total_bedrooms
#                     row[7] = total_sqft
#                     row[8] = number_of_RI_properties
#                     row[9] = number_of_EXGD_properties
#                     row[10] = number_of_AV_properties
#                     row[11] = number_of_FRPRUN_properties
#                     row[12] = number_of_safe_fdn_properties
#                     county_code = row[13]
#                     cousub_name = row[14]
#                     cursor.updateRow(row)
#
#                     # write to csv
#                     writer.writerow(
#                         [year, projection, state_number_inund, county_code, cousub_name, number_of_properties, "%.2f" % total_ass_val,
#                          "%.2f" % total_tax,
#                          ave_ass_val, ave_yrbuilt, total_bedrooms, total_sqft, loanduedate_notnull,
#                          number_of_RI_properties,
#                          number_of_EXGD_properties, number_of_AV_properties, number_of_FRPRUN_properties,
#                          number_of_safe_fdn_properties])
#                     print 'Wrote to csv'

def strip_quotes(filename):
    with open(filename, 'rt') as f:
        data = f.read()
    new_data = data.replace('"','')
    csv_filename = 'C:/Users/kristydahl/Desktop/GIS_data/zillow/state_property_csvs/test_state_36_sample_geocoded_geocodio_stripped.csv'
    with open(csv_filename, 'wb') as csvfile:
        writer = csv.writer(csvfile)
        for row in csv.reader(new_data.splitlines(), delimiter=',', skipinitialspace=True):
            writer.writerow(row)

#strip_quotes('C:/Users/kristydahl/Desktop/GIS_data/zillow/state_property_csvs/test_state_36_sample_geocoded_geocodio_stripquotes.csv')
#SEQUENCE OF COMMANDS SET 1: IDENTIFYING PROPERTIES TO BE GEOCODED. RUN IN ARCMAP
#csv_to_featureclass('6','west_coast')
#identify_properties_to_be_geocoded('6','west_coast','2100','NCAH')

# SEQUENCE OF COMMANDS SET 2: POST-GEOCODIO
#create_csv_from_pre_geocode_featureclass('12')
# join_geocodio_data_and_pre_geocode_data('12')
# join_and_merge_for_final_properties_dataset('12','east_coast')

# SEQUENCE OF COMMANDS SET 3: IDENTIFY CI PROPERTIES. RUN IN ARCMAP
#erase_properties_within_levees('12')
#identify_ci_properties_fl('12','east_coast',['2030','2045','2060','2080'],'NCAH')

# SEQUENCE OF COMMANDS SET 3: FIRST TWO ONLY NEED TO BE RUN ONCE PER SCENARIO
add_fields_to_geography_layer(['2006','2100'],'NCAH','states')
# add_fields_to_geography_layer(['2006','2030','2045','2060','2080','2100'],'NCAH','cousubs')
#output_state_statistics('9',['2006','2030','2045','2060','2080','2100'],'NCAH')
#output_county_subdivision_statistics('24',['2006','2030','2045','2060','2080','2100'],'NCAH')
output_statistics_by_geography_type('44','state',['2006','2100'],'NCAH')
#state_numbers = ['33']


# for state_number in state_numbers:
#     print 'Analyzing state: ' + state_number
#     join_geocodio_data_and_pre_geocode_data(state_number)
#     join_and_merge_for_final_properties_dataset(state_number,'east_coast')
#     erase_properties_within_levees(state_number)
#     identify_ci_properties(state_number,'east_coast',['2006','2030','2045','2060','2080','2100'],'NCAH')
#     output_state_statistics(state_number,['2006','2030','2045','2060','2080','2100'],'NCAH')
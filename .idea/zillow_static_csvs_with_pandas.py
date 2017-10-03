import pandas
import csv
import urllib
import json

def join_ztrax_tables(state_number):
    # specify folder where zillow data layouts live
    layout_folder = '/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/layouts/'

    # specify layout files
    ztrans_main_layout = pandas.read_csv(str(layout_folder + 'ztrans_main_layout.csv'), sep='\t', header=None)
    ztrans_propinfo_layout = pandas.read_csv(layout_folder + 'ztrans_propinfo_layout.csv', sep='\t', header=None)

    zasmt_main_layout = pandas.read_csv(layout_folder + 'zasmt_main_layout.csv', sep='\t', header=None)
    zasmt_value_layout = pandas.read_csv(layout_folder + 'zasmt_value_layout.csv', sep='\t', header=None)
    zasmt_building_layout = pandas.read_csv(layout_folder + 'zasmt_building_layout.csv', sep=',', header=None)

    # specify folder where state-specific data lives
    data_folder = '/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/{0}/' .format(state_number)

    # specify subfolders and file names
    ztrans_folder = data_folder + 'Ztrans/'
    zasmt_folder = data_folder + 'Zasmt/'

    # specify data files
    ztrans_main_file = ztrans_folder + 'Main.txt'
    ztrans_propinfo_file = ztrans_folder + 'PropertyInfo.txt'

    zasmt_main_file = zasmt_folder + 'Main.txt'
    zasmt_value_file = zasmt_folder + 'Value.txt'
    zasmt_building_file = zasmt_folder + 'Building.txt' # check this file name--haven't put in dropbox yet

    # turn layout series into lists of column names
    ztrans_main_colnames = ztrans_main_layout[1].tolist()
    ztrans_propinfo_colnames = ztrans_propinfo_layout[1].tolist()

    zasmt_main_colnames = zasmt_main_layout[1].tolist()
    zasmt_value_colnames = zasmt_value_layout[1].tolist()
    zasmt_building_colnames = zasmt_building_layout[1].tolist()

    # read in files, specifying column names
    ztrans_main_all = pandas.read_table(ztrans_main_file,header=None,sep='|',names=ztrans_main_colnames,dtype='object')

    print 'read in ztrans main'

    ztrans_main = ztrans_main_all[['TransId','LoanRateTypeStndCode','LoanDueDate']]

    ztrans_propinfo_all = pandas.read_table(ztrans_propinfo_file,header=None,sep='|',names=ztrans_propinfo_colnames,dtype='object')
    ztrans_propinfo = ztrans_propinfo_all[['TransId','ImportParcelID','PropertyAddressUnitDesignator','PropertyAddressUnitNumber']]

    zasmt_main_all = pandas.read_table(zasmt_main_file,header=None,sep='|',names=zasmt_main_colnames, dtype='object')
    zasmt_main = zasmt_main_all[['RowID','ImportParcelID','FIPS','PropertyFullStreetAddress','PropertyCity','PropertyState','PropertyZip','LoadID','PropertyAddressLatitude','PropertyAddressLongitude','TaxAmount']] # is this all I need?

    zasmt_value_all = pandas.read_table(zasmt_value_file,header=None,sep='|',names=zasmt_value_colnames, dtype='object')
    zasmt_value = zasmt_value_all[['RowID','TotalAssessedValue','AssessmentYear','TotalMarketValue','MarketValueYear','TotalAppraisalValue','AppraisalValueYear']] # is this all I need?

    zasmt_building_all = pandas.read_table(zasmt_building_file,header=None,sep='|',names=zasmt_building_colnames, dtype='object')
    zasmt_building = zasmt_building_all[['RowID','NoOfUnits','BuildingConditionStndCode','FoundationTypeStndCode','TotalBedrooms','PropertyLandUseStndCode','YearBuilt','EffectiveYearBuilt']]

    # join asmt data
    join_zasmt_main_zasmt_value = pandas.merge(zasmt_main, zasmt_value, on='RowID')
    join_zasmt_main_zasmt_value_zasmt_building = pandas.merge(join_zasmt_main_zasmt_value, zasmt_building, on='RowID')

    # join ztrans data
    join_ztrans_main_ztrans_propinfo = pandas.merge(ztrans_main, ztrans_propinfo, on='TransId')

    # join zasmt to ztrans data
    join_all = pandas.merge(join_ztrans_main_ztrans_propinfo, join_zasmt_main_zasmt_value_zasmt_building, on='ImportParcelID')

    print 'Join_all is this long: ' + str(len(join_all))
    return join_all

def select_properties_in_coastal_counties(state_number):
    # fix data names for datafile
    joined_datafile = join_ztrax_tables(state_number)
    df_of_fips_codes = pandas.read_csv('/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/coastal_counties_fips_name.csv',sep=',',header=0, dtype='str')
    list_of_fips_codes = df_of_fips_codes['FIPS'].values.tolist()

    # select only properties in coastal counties
    properties_in_coastal_counties = joined_datafile[joined_datafile['FIPS'].isin(list_of_fips_codes)]

    return properties_in_coastal_counties
    print 'Selected properties in coastal counties in state {0}' .format(state_number)


# This method weeds out duplicates. ZTRAX sample code suggested doing so by LoadID, but all duplicate properties (by ImportParcelID) in RI dataset had the same LoadID, same assessed value, etc.
# So this method just takes the last instance of each parcel by ParcelID
def weed_out_dupes(state_number):
    datafile_from_select = select_properties_in_coastal_counties(state_number)
    no_dupes = datafile_from_select.drop_duplicates(subset='ImportParcelID', keep='last')

    #fix data names for output. Once this has had more testing, consider having this script just return the no_dupes file, then call the weed_out_dupes function in the geocode function

    #no_dupes.to_csv('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/testing_44_joined_data_nodupes_by_drop_dupes_092017-2.csv',index=False, sep=',',encoding='utf-8')

    return no_dupes
    print 'Weeded out dupes for state {0}' .format(state_number)


def geocode(state_numbers):
    for state_number in state_numbers:
        datafile_no_dupes = weed_out_dupes(state_number)
        api_key = X ## SPECIFY KEY HERE

        null_rows = datafile_no_dupes.loc[datafile_no_dupes['PropertyAddressLatitude'].isnull()]
        not_null_rows = datafile_no_dupes.loc[datafile_no_dupes['PropertyAddressLatitude'].notnull()]

        # when not testing, remove the test_data line and change the for loop to read directly from null_rows
        test_data = null_rows.iloc[0:5]

        for index, row in test_data.iterrows():
            ImportParcelID = row.ImportParcelID
            street_address = row.PropertyFullStreetAddress.replace(' ','+')
            city = row.PropertyCity.replace(' ','+')
            lat_field = row.PropertyAddressLatitude
            long_field = row.PropertyAddressLatitude

            # prep query and url for api call
            address_query = str(street_address + '+' + city + '+' + row.PropertyState)
            url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' + address_query + '&key=' + api_key

            # call api and parse response
            response = urllib.urlopen(url)
            json_response = json.loads(response.read())

            if json_response['results'] == []:
                print 'This location does not exist'
            else:
                geocoded_lat = json_response['results'][0]['geometry']['location']['lat']
                geocoded_long = json_response['results'][0]['geometry']['location']['lng']

                # update the data frame of null_rows with the geocoded lats and longs. do i want to just update the original datafile csv??
                null_rows.loc[null_rows['ImportParcelID'] == ImportParcelID, 'PropertyAddressLatitude'] = geocoded_lat
                null_rows.loc[null_rows['ImportParcelID'] == ImportParcelID, 'PropertyAddressLongitude'] = geocoded_long
                print 'Updated latitude and longitude for ImportParcelID ' + str(ImportParcelID)

        # concatenate the files that had geocoded locations from zillow with those geocoded in this method
        null_rows_now_geocoded = null_rows.loc[null_rows['PropertyAddressLatitude'].notnull()]
        updated_datafile = pandas.concat([null_rows_now_geocoded, not_null_rows])
        updated_datafile.to_csv('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/state_{0}_clean_geocoded_data.csv' .format(state_number),sep=',',encoding='utf-8', index=False)

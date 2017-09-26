import pandas
import csv
import urllib
import json
import psycopg2
import gc
import codecs
import datetime

def connect_to_db():
    conn_string = "host='localhost' dbname='test_zillow' user='kristinadahl' password='latte4me'"

    # print the connection string we will use to connect
    print "Connecting to database\n	->%s" % (conn_string)

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    print "Connected!\n"

#def create_tables_in_db(state_numbers):

    return conn_string

def create_tables(state_number):
    conn_string = connect_to_db()

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    commands = (
        """
        CREATE TABLE state_{0}_ztrans_main (
            trans_id BIGINT PRIMARY KEY NOT NULL,
            fips TEXT NOT NULL,
            loanratetypestndcode TEXT,
            loanduedate DATE
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_ztrans_propinfo (
            transid TEXT PRIMARY KEY NOT NULL,
            importparcelid BIGINT NOT NULL,
            propertyaddressunitdesignator TEXT,
            propertyaddressunitnumber TEXT
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_zasmt_buildingarea (
            rowid BIGINT PRIMARY KEY NOT NULL,
            buildingareasqft BIGINT
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_zasmt_main (
            rowid BIGINT PRIMARY KEY NOT NULL,
            importparcelid BIGINT NOT NULL,
            fips TEXT NOT NULL,
            propertyfullstreetaddress TEXT,
            propertycity TEXT,
            propertystate TEXT,
            propertyzip TEXT,
            loadid BIGINT,
            propertyaddresslatitude TEXT,
            propertyaddresslongitude TEXT,
            taxamount NUMERIC
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_zasmt_value (
            rowid BIGINT PRIMARY KEY NOT NULL,
            totalassessedvalue NUMERIC,
            assessmentyear INT,
            totalmarketvalue NUMERIC,
            marketvalueyear INT,
            totalappraisalvalue NUMERIC,
            appraisalvalueyear INT
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_zasmt_building (
            rowid BIGINT PRIMARY KEY NOT NULL,
            noofunits INT,
            buildingconditionstndcode TEXT,
            foundationtypestndcode TEXT,
            totalbedrooms INT,
            propertylandusestndcode TEXT,
            yearbuilt TEXT,
            effectiveyearbuilt TEXT
        ) WITH OIDS
        """ .format(state_number)
    )

    for command in commands:
        cursor.execute(command)
        cursor.close
        conn.commit()


def check_if_field_is_null(fields):
    for field in fields:
        print 'checking ' + str(field)
        if field in['', '\x00',' ']:
            field = None
            print str(field) + ' is empty'
        else:
            field = field
            print str(field) + ' has data'

def read_in_ztrans_main_csv_data_and_insert_rows(filename):

    # connect to db
    conn_string = connect_to_db()

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    df_of_fips_codes = pandas.read_csv(
        '/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/coastal_counties_fips_name.csv', sep=',', header=0,
        dtype='str')
    list_of_fips_codes = df_of_fips_codes['FIPS'].values.tolist()

    with open(filename) as infile:
        for index, row in enumerate(infile):
            split_row = row.split('|')
            TransId = int(split_row[0])
            FIPS = split_row[1]
            LoanRateTypeStndCode = split_row[66]
            LoanDueDate = split_row[67]

            fields = [LoanRateTypeStndCode, LoanDueDate]

            print fields
            print 'checking fields'
            check_if_field_is_null(fields)

            print 'checked fields'

            # if split_row[66] in['', '\x00',' ']:
            #     LoanRateTypeStndCode = None
            # else:
            #     LoanRateTypeStndCode = split_row[66]
            #
            # if split_row[67] in['', '\x00',' ']:
            #     LoanDueDate = None
            # else:
            #     LoanDueDate = split_row[67]

            print 'Loan rate is: ' + LoanRateTypeStndCode
            print 'Loan due is: ' + LoanDueDate

            print 'Inserting rows'
            if FIPS in list_of_fips_codes:
                cursor.execute("INSERT INTO state_11_ztrans_main (trans_id, fips, loanratetypestndcode, loanduedate) VALUES (%s, %s, %s, %s)", (TransId, FIPS, LoanRateTypeStndCode, LoanDueDate))       #specify values to insert into table
                conn.commit()

# this gives error "_csv.Error: line contains NULL byte"
def use_csv_pkg(filename):

    with open(filename) as infile:
        for index, row in enumerate(infile):
            print index
            split_row = row.split('|')
            print split_row[0]
            print split_row[66] # this returns empty, which  i think will be ok for inserting into table

# this fails on the 60,000th row
def read_in_csv_data_in_chunks_and_write_to_new_csv(filename):
    df_of_fips_codes = pandas.read_csv(
        '/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/coastal_counties_fips_name.csv', sep=',', header=0,
        dtype='str')
    list_of_fips_codes = df_of_fips_codes['FIPS'].values.tolist()


    chunksize = 5000

    #with open('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/06/ZTrans/Main_FIPS.csv', 'w') as csvfile_to_write:

    #datawriter = csv.writer(csvfile_to_write, delimiter=',')

    df_iter = pandas.read_csv(filename, chunksize=chunksize, delimiter='|', dtype={0: int, 1: int, 66: str, 67: object}, usecols=[0,1,66,67], names=['TransId', 'FIPS', 'LoanRateTypeStndCode', 'LoanDueDate'],header=None)

    for row in df_iter:

        print row
        TransId = row[0]
        FIPS = row[1]
        LoanRateTypeStndCode = row[2]
        LoanDueDate = row[3]

    # for chunk in chunks:
    #     for index, row in chunk.iterrows():
    #
    #         for row in chunks:
    #             TransId = row[0]
    #             FIPS = row[1]
    #             LoanRateTypeStndCode = row[2]
    #             LoanDueDate = row[3]
    #             #print index
    #
    #             # print TransId
    #             # print FIPS
    #             # print LoanRateTypeStndCode
    #             # print LoanDueDate
    #
    #             if FIPS in list_of_fips_codes:
    #                 print row
    #                 print to_write
    #                 pandas.df.append(TransId, FIPS, LoanRateTypeStndCode, LoanDueDate)
    #             print df
    #             # pandas.df.to_csv('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/06/ZTrans/Main_FIPS.csv')
    #             #         #datawriter.writerow([TransId, FIPS, LoanRateTypeStndCode, LoanDueDate])
    #
    #             del df
    #             gc.collect()
    #             del gc.garbage[:]

def getstuff(filename):
    df_of_fips_codes = pandas.read_csv(
            '/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/coastal_counties_fips_name.csv', sep=',', header=0,
            dtype={'FIPS': int, 'Name': str})
    list_of_fips_codes = df_of_fips_codes['FIPS'].values.tolist()

    with open(filename,"rb") as csvfile:
        datareader = pandas.read_csv(csvfile, nrows=10,delimiter='|',dtype={0: int, 1: int, 66: str, 67: object}, usecols=[0,1,66,67])
        count = 0

        for row in datareader:
            #print row
            if row[1] in list_of_fips_codes:
                yield row
            else:
                return

def getdata(filename):
    for row in getstuff(filename):
        yield row

def printdata(filename):
    for row in getdata(filename):
        print row

state_numbers = ['06']
# Add Building Area table and layout so we can pull in BuildingAreaSqFt
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

    boom

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
        api_key = 'AIzaSyD5oyVjuyaP-6UdEbbxw_sD-2UiNchUDFc'

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

#create_tables('11')
read_in_ztrans_main_csv_data_and_insert_rows('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/11/11/ZTrans/Main.txt')
#read_in_csv_data_in_chunks_and_insert_rows('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/06/ZTrans/Main.txt')
#use_csv_pkg('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/06/ZTrans/Main.txt')
#create_tables('11')











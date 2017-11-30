import pandas
import csv
import urllib
import json
import psycopg2
import gc
import codecs
from datetime import datetime

# Specify paths to data
paths = pandas.read_csv('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/paths_to_data.csv', sep=',', header=None,dtype='str')

folder_path = paths[0][0]
data_path = paths[1][0]

# Connect to psql database
def connect_to_db():
    with open(folder_path + 'db_config.csv') as config_file:
        csvreader = csv.reader(config_file, delimiter=',')
        for row in csvreader:
            host = row[0]
            dbname = row[1]
            user = row[2]
            password = row[3]

    #conn_string = "host='localhost' dbname='test_zillow' user='kristinadahl' password='latte4me'"
    conn_string = "host={0} dbname={1} user={2} password={3}" .format(host, dbname, user, password)

    # print the connection string we will use to connect
    print "Connecting to database\n	->%s" % (conn_string)

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    print "Connected!\n"
    return conn_string

# Create tables to store ZTRAX data
def create_tables(state_number):
    conn_string = connect_to_db()

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    commands = (
        """
        CREATE TABLE state_{0}_ztrans_main (
            id SERIAL,
            transid TEXT PRIMARY KEY NOT NULL,
            loadid BIGINT NOT NULL,
            fips TEXT NOT NULL,
            loanratetypestndcode TEXT,
            loanduedate TEXT
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_ztrans_propinfo (
            id SERIAL,
            transid TEXT NOT NULL,
            importparcelid BIGINT,
            loadid BIGINT NOT NULL,
            propertyaddressunitdesignator TEXT,
            propertyaddressunitnumber TEXT,
            propertysequencenumber INT,
            propertyaddresscensustractandblock TEXT,
            fips TEXT NOT NULL
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_zasmt_buildingareas (
            id SERIAL,
            rowid TEXT NOT NULL,
            buildingareasqft BIGINT,
            buildingareastndcode TEXT,
            fips TEXT
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_zasmt_main (
            id SERIAL,
            rowid TEXT NOT NULL,
            importparcelid BIGINT PRIMARY KEY NOT NULL,
            fips TEXT NOT NULL,
            propertyhousenumber TEXT,
            propertyfullstreetaddress TEXT,
            propertycity TEXT,
            propertystate TEXT,
            propertyzip TEXT,
            loadid BIGINT,
            propertyaddresslatitude TEXT,
            propertyaddresslongitude TEXT,
            taxamount NUMERIC,
            created TIMESTAMP not null DEFAULT CURRENT_TIMESTAMP,
            updated TIMESTAMP not null DEFAULT CURRENT_TIMESTAMP
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_zasmt_value (
            id SERIAL,
            rowid TEXT PRIMARY KEY NOT NULL,
            totalassessedvalue NUMERIC,
            assessmentyear TEXT,
            totalmarketvalue NUMERIC,
            marketvalueyear TEXT,
            totalappraisalvalue NUMERIC,
            appraisalvalueyear TEXT,
            fips TEXT
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_zasmt_building (
            id SERIAL,
            rowid TEXT PRIMARY KEY NOT NULL,
            noofunits INT,
            buildingconditionstndcode TEXT,
            foundationtypestndcode TEXT,
            totalbedrooms INT,
            propertylandusestndcode TEXT,
            yearbuilt TEXT,
            effectiveyearbuilt TEXT,
            fips TEXT
        ) WITH OIDS
        """ .format(state_number)
    )

    for command in commands:
        print command
        cursor.execute(command)
        cursor.close
        conn.commit()

# Check if a field is null before inserting into database table
def check_if_field_is_null(field):
        if field in['', '\x00',' ']:
            return None
        else:
            return field

# Methods that insert Zillow data into database tables only insert properties located within coastal counties as determined by FIPS code
# Read Ztrans main csv data and insert into database table
def read_ztrans_main_csv_data_and_insert_rows(state_number):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    filename = data_path + str(state_number) + '/' + 'Ztrans/Main.txt'
    # get list of fips codes
    df_of_fips_codes = pandas.read_csv('/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/coastal_counties_fips_name.csv', sep=',', header=0,dtype='str')
    list_of_fips_codes = df_of_fips_codes['FIPS'].values.tolist()

    print 'Ztrans main starting'
    # read ztrans_main csv file and insert rows into corresponding table
    with open(filename) as infile:
        for index, row in enumerate(infile):
            split_row = row.split('|')
            TransId = int(split_row[0])
            FIPS = split_row[1]
            LoanRateTypeStndCode = split_row[66]
            LoanDueDate = split_row[67]
            LoadId = split_row[124]

            field = LoanRateTypeStndCode
            LoanRateTypeStndCode = check_if_field_is_null(field)

            field = LoanDueDate
            LoanDueDate = check_if_field_is_null(field)

            if FIPS in list_of_fips_codes:
                cursor.execute("""INSERT INTO state_%s_ztrans_main (transid, loadid, fips, loanratetypestndcode, loanduedate) VALUES (%s, %s, %s, %s, %s)""", (state_number, TransId, LoadId, FIPS, LoanRateTypeStndCode, LoanDueDate))
                conn.commit()

    print 'Ztrans main done'


# Read Ztrans propertyinfo csv data and insert into database table
def read_ztrans_propinfo_csv_data_and_insert_rows(state_number):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    filename = data_path + str(state_number) + '/' + 'Ztrans/PropertyInfo.txt'
    df_of_fips_codes = pandas.read_csv('/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/coastal_counties_fips_name.csv', sep=',', header=0,dtype='str')
    list_of_fips_codes = df_of_fips_codes['FIPS'].values.tolist()


    print 'Ztrans propinfo starting'
    # read ztrans_propinfo csv file and insert rows into corresponding table
    with open(filename) as infile:
        for index, row in enumerate(infile):
            split_row = row.split('|')
            TransId = int(split_row[0])
            ImportParcelId = split_row[64]
            PropertySequenceNumber = split_row[46]
            PropertyAddressUnitDesignator = split_row[48]
            PropertyAddressUnitNumber = split_row[49]
            FIPS = split_row[62]
            LoadId = split_row[63]
            PropertyAddressCensusTractAndBlock = split_row[54]

            field = ImportParcelId
            ImportParcelId = check_if_field_is_null(field)

            field = PropertySequenceNumber
            PropertySequenceNumber = check_if_field_is_null(field)

            field = PropertyAddressUnitDesignator
            PropertyAddressUnitDesignator = check_if_field_is_null(field)

            field = PropertyAddressUnitNumber
            PropertyAddressUnitNumber = check_if_field_is_null(field)

            field = PropertyAddressCensusTractAndBlock
            PropertyAddressCensusTractAndBlock = check_if_field_is_null(field)

            if FIPS in list_of_fips_codes:
                cursor.execute(
                    "INSERT INTO state_%s_ztrans_propinfo (transid, importparcelid, loadid, propertysequencenumber, propertyaddressunitdesignator, propertyaddressunitnumber, fips,propertyaddresscensustractandblock) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)" , (state_number, TransId, ImportParcelId, LoadId, PropertySequenceNumber, PropertyAddressUnitDesignator, PropertyAddressUnitNumber, FIPS, PropertyAddressCensusTractAndBlock))

                conn.commit()

    cursor.execute(
        "CREATE TABLE state_{0}_ztrans_propinfo_nodupes AS SELECT * FROM (SELECT row_number() OVER (PARTITION BY importparcelid ORDER BY loadid desc) AS rn , * "
        "FROM state_{0}_ztrans_propinfo) AS SubQueryAlias WHERE rn = 1" .format(state_number))

    conn.commit()

    print 'Ztrans propinfo done'

# Read Zasmt main csv data and insert into database table
def read_zasmt_main_csv_data_and_insert_rows(state_number):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    filename = data_path + str(state_number) + '/' + 'ZAsmt/Main.txt'
    # get list of fips codes
    df_of_fips_codes = pandas.read_csv('/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/coastal_counties_fips_name.csv', sep=',', header=0,dtype='str')
    list_of_fips_codes = df_of_fips_codes['FIPS'].values.tolist()

    print 'Zasmt main starting'
    # read ztrans_main csv file and insert rows into corresponding table
    with open(filename) as infile:
        for index, row in enumerate(infile):
            split_row = row.split('|')
            RowId = split_row[0]
            ImportParcelId = split_row[1]
            FIPS = split_row[2]
            PropertyHouseNumber = split_row[20]
            PropertyFullStreetAddress= split_row[26]
            PropertyCity = split_row[27]
            PropertyState = split_row[28]
            PropertyZip = split_row[29]
            PropertyAddressLatitude = split_row[81]
            PropertyAddressLongitude = split_row[82]
            TaxAmount = split_row[38]
            LoadId = split_row[75]

            field = PropertyFullStreetAddress
            PropertyFullStreetAddress = check_if_field_is_null(field)

            field = PropertyCity
            PropertyCity = check_if_field_is_null(field)

            field = PropertyState
            PropertyState = check_if_field_is_null(field)

            field = PropertyZip
            PropertyZip = check_if_field_is_null(field)

            field = PropertyAddressLatitude
            PropertyAddressLatitude = check_if_field_is_null(field)

            field = PropertyAddressLongitude
            PropertyAddressLongitude = check_if_field_is_null(field)

            field = TaxAmount
            TaxAmount = check_if_field_is_null(field)

            field = LoadId
            LoadId = check_if_field_is_null(field)

            field = PropertyHouseNumber
            PropertyHouseNumber = check_if_field_is_null(field)

            if FIPS in list_of_fips_codes:
                cursor.execute("INSERT INTO state_%s_zasmt_main (rowid, importparcelid, fips, propertyhousenumber, propertyfullstreetaddress, propertycity, propertystate, "
                               "propertyzip, propertyaddresslatitude, propertyaddresslongitude, taxamount, loadid"
                               ") "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (state_number, RowId, ImportParcelId, FIPS, PropertyHouseNumber, PropertyFullStreetAddress,
                                                                                           PropertyCity, PropertyState, PropertyZip, PropertyAddressLatitude, PropertyAddressLongitude,
                                                                                           TaxAmount, LoadId))
                conn.commit()

    print 'Zasmt main done'

# Read Zasmt value csv data and insert into database table
def read_zasmt_value_csv_data_and_insert_rows(state_number):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    filename = data_path + str(state_number) + '/' + 'ZAsmt/Value.txt'

    # get list of fips codes
    df_of_fips_codes = pandas.read_csv('/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/coastal_counties_fips_name.csv', sep=',', header=0,dtype='str')
    list_of_fips_codes = df_of_fips_codes['FIPS'].values.tolist()

    print 'Zasmt value starting'
    # read asmt_value csv file and insert rows into corresponding table
    with open(filename) as infile:
        for index, row in enumerate(infile):
            split_row = row.split('|')
            RowId = split_row[0]
            TotalAssessedValue = split_row[3]
            AssessmentYear= split_row[4]
            TotalMarketValue= split_row[7]
            MarketValueYear = split_row[8]
            TotalAppraisalValue = split_row[11]
            AppraisalValueYear = split_row[12]
            FIPS = split_row[13]

            field = TotalAssessedValue
            TotalAssessedValue = check_if_field_is_null(field)

            field = AssessmentYear
            AssessmentYear = check_if_field_is_null(field)

            field = TotalMarketValue
            TotalMarketValue = check_if_field_is_null(field)

            field = MarketValueYear
            MarketValueYear = check_if_field_is_null(field)

            field = TotalAppraisalValue
            TotalAppraisalValue = check_if_field_is_null(field)

            field = AppraisalValueYear
            AppraisalValueYear = check_if_field_is_null(field)

            if FIPS in list_of_fips_codes:
                cursor.execute("INSERT INTO state_%s_zasmt_value (rowid, totalassessedvalue, assessmentyear, totalmarketvalue, marketvalueyear, totalappraisalvalue, appraisalvalueyear, fips"
                               ") "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)" , (state_number, RowId, TotalAssessedValue, AssessmentYear, TotalMarketValue, MarketValueYear, TotalAppraisalValue, AppraisalValueYear, FIPS))
                conn.commit()
    print 'Zasmt value done'

# Read Zasmt building csv data and insert into database table
def read_zasmt_building_csv_data_and_insert_rows(state_number):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    filename = data_path + str(state_number) + '/' + 'ZAsmt/Building.txt'

    # get list of fips codes
    df_of_fips_codes = pandas.read_csv('/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/coastal_counties_fips_name.csv', sep=',', header=0,dtype='str')
    list_of_fips_codes = df_of_fips_codes['FIPS'].values.tolist()

    print 'Zasmt building starting'
    # read asmt_value csv file and insert rows into corresponding table
    with open(filename) as infile:
        for index, row in enumerate(infile):
            split_row = row.split('|')
            RowId = split_row[0]
            NoOfUnits = split_row[1]
            BuildingConditionStndCode = split_row[12]
            FoundtationTypeStndCode = split_row[33]
            TotalBedrooms = split_row[19]
            PropertyLandUseStndCode = split_row[5]
            YearBuilt = split_row[14]
            EffectiveYearBuilt = split_row[15]
            FIPS = split_row[45]

            field = NoOfUnits
            NoOfUnits = check_if_field_is_null(field)

            field = BuildingConditionStndCode
            BuildingConditionStndCode = check_if_field_is_null(field)

            field = FoundtationTypeStndCode
            FoundtationTypeStndCode = check_if_field_is_null(field)

            field = TotalBedrooms
            TotalBedrooms = check_if_field_is_null(field)

            field = PropertyLandUseStndCode
            PropertyLandUseStndCode = check_if_field_is_null(field)

            field = YearBuilt
            YearBuilt = check_if_field_is_null(field)

            field = EffectiveYearBuilt
            EffectiveYearBuilt = check_if_field_is_null(field)

            if FIPS in list_of_fips_codes:
                cursor.execute("INSERT INTO state_%s_zasmt_building (rowid, noofunits, buildingconditionstndcode, foundationtypestndcode, totalbedrooms, propertylandusestndcode, yearbuilt, effectiveyearbuilt, fips"
                               ") "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)" ,
                               (state_number, RowId, NoOfUnits, BuildingConditionStndCode, FoundtationTypeStndCode, TotalBedrooms, PropertyLandUseStndCode, YearBuilt, EffectiveYearBuilt, FIPS))
                conn.commit()
    print 'Zasmt building done'

# Read Zasmt buildingareas csv data and insert into database table
def read_zasmt_buildingareas_csv_data_and_insert_rows(state_number):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    filename = data_path + str(state_number) + '/' + 'ZAsmt/BuildingAreas.txt'

    # get list of fips codes
    df_of_fips_codes = pandas.read_csv('/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/coastal_counties_fips_name.csv', sep=',', header=0,dtype='str')
    list_of_fips_codes = df_of_fips_codes['FIPS'].values.tolist()

    print 'Zasmt buildingareas starting'
    # read asmt_value csv file and insert rows into corresponding table
    # only add rows that represent the total building area, as sometimes a property is listed multiple times based on different measures of building area
    with open(filename) as infile:
        for index, row in enumerate(infile):
            split_row = row.split('|')
            RowId = split_row[0]
            BuildingAreaSqFt = split_row[4]
            FIPS = split_row[5]
            BuildingAreaStndCode = split_row[3]

            field = BuildingAreaSqFt
            BuildingAreaSqFt = check_if_field_is_null(field)

            if FIPS in list_of_fips_codes:
                if BuildingAreaStndCode in['BAT','BAG','BAE','BAF','BLF','BAJ']:
                    cursor.execute("INSERT INTO state_%s_zasmt_buildingareas (rowid, buildingareasqft, buildingareastndcode, fips"
                                   ") "
                                   "VALUES (%s, %s, %s, %s)" , (state_number, RowId, BuildingAreaSqFt, BuildingAreaStndCode, FIPS))
                    conn.commit()
    print 'Zasmt building areas done'

def join_data(state_number):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # Specify names of tables to create
    asmt_table_to_create = 'state_' + str(state_number) + '_joined_asmt_data'
    trans_table_to_create = 'state_' + str(state_number) + '_joined_trans_data'
    joined_table_to_create = 'state_' + str(state_number) + '_joined_data'
    zasmt_main = 'state_' + str(state_number) + '_zasmt_main'
    zasmt_value = 'state_' + str(state_number) + '_zasmt_value'
    zasmt_building = 'state_' + str(state_number) + '_zasmt_building'
    zasmt_buildingareas = 'state_' + str(state_number) + '_zasmt_buildingareas'
    ztrans_main = 'state_' + str(state_number) + '_ztrans_main'
    ztrans_propinfo = 'state_' + str(state_number) + '_ztrans_propinfo_nodupes'

    # Join Zasmt tables based on rowid
    cursor.execute("CREATE TABLE {0} AS SELECT {1}.rowid, {1}.loadid, {1}.importparcelid, {1}.propertyfullstreetaddress, {1}.propertycity, {1}.propertystate, "
                   "{1}.propertyzip, {1}.propertyhousenumber, {1}.propertyaddresslatitude, {1}.propertyaddresslongitude, {1}.taxamount, {1}.created, {1}.updated, "
                   "{2}.totalassessedvalue, {2}.assessmentyear, {2}.totalmarketvalue, {2}.marketvalueyear,"
                   "{2}.totalappraisalvalue, {2}.appraisalvalueyear,"
                   "{3}.noofunits, {3}.buildingconditionstndcode, {3}.foundationtypestndcode, {3}.totalbedrooms,"
                   "{3}.propertylandusestndcode, {3}.yearbuilt, {3}.effectiveyearbuilt, "
                   "{4}.buildingareasqft, {4}.buildingareastndcode "
                   "FROM {1} LEFT JOIN {2} on {1}.rowid = {2}.rowid "
                   "LEFT JOIN {3} on {2}.rowid = {3}.rowid "
                   "LEFT JOIN {4} on {3}.rowid = {4}.rowid; "
                    .format(asmt_table_to_create, zasmt_main, zasmt_value, zasmt_building, zasmt_buildingareas))
    print 'Joined ZAsmt data'

    # Join Ztrans tables based on transid
    cursor.execute("CREATE TABLE {0} AS SELECT {1}.transid, {1}.importparcelid, {1}.propertyaddressunitdesignator, {1}.propertyaddressunitnumber, {1}.propertyaddresscensustractandblock, "
                   "{2}.fips, {2}.loadid, {2}.loanratetypestndcode, {2}.loanduedate, {1}.propertysequencenumber "
                   "FROM {1} LEFT JOIN {2} on {2}.transid = {1}.transid; "
                   .format(trans_table_to_create, ztrans_propinfo, ztrans_main))
    conn.commit()
    print 'Joined ZTrans data'

    # Join Ztrans and Zasmt tables based on importparcelid
    cursor.execute("CREATE TABLE {0} AS SELECT {1}.*, {2}.propertyaddressunitdesignator, {2}.propertyaddressunitnumber, {2}.loanratetypestndcode, {2}.loanduedate, "
                   "{2}.propertysequencenumber, {2}.propertyaddresscensustractandblock "
                   "FROM {1} LEFT JOIN {2} on {2}.importparcelid = {1}.importparcelid; "
                   .format(joined_table_to_create, asmt_table_to_create, trans_table_to_create))

    # Add geocoded field to joined data table
    cursor.execute("ALTER TABLE {0} ADD geocoded TEXT;" .format(joined_table_to_create))
    conn.commit()
    print 'Joined all data!'

# Delete rows that have no house number or existing latitude field (these cannot be accurately geocoded)
def delete_rows(state_number):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM state_{0}_joined_data WHERE propertyhousenumber IS NULL and propertyaddresslatitude IS NULL; " .format(state_number))
    conn.commit()

    print 'Deleted rows'

# Delete any duplicate rows from the joined data table based on whether they have the same rowid, importparcelid, and loadid, keeping the one with the highest buildingareasqft
def delete_dupes_from_joined_data(state_numbers):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    for state_number in state_numbers:
        cursor.execute("CREATE TABLE state_{0}_joined_data_nodupes AS SELECT * FROM (SELECT row_number() "
                       "OVER (PARTITION BY rowid, importparcelid, loadid ORDER BY buildingareasqft desc) AS rn , * "
                       "FROM state_{0}_joined_data) AS SubQueryAlias WHERE rn = 1;" .format(state_number))

        conn.commit()
        print 'Deleted dupes from state ' + str(state_number)


# Get state abbreviation from state number
def get_state_abbreviation(state_number):
    state_numbers_abbrs = '/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/state_numbers_abbrs.csv'

    with open(state_numbers_abbrs,'rb') as csvfile:

        csvreader = csv.reader(csvfile, delimiter=',')
        for row in csvreader:
            if str(row[0]) == str(state_number):
                state = str(row[1])
    print 'State is: ' + state

    return(state)

# Fill in state field with state abbreviation if state field is null
def update_state_field_if_null(state_number):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    state = get_state_abbreviation(state_number)

    # Update state field (make text file with state #, two-letter abbreviation
    cursor.execute("SELECT * FROM state_{0}_joined_data WHERE propertystate IS NULL" .format(state_number))
    rows = cursor.fetchall()
    print 'There are ' + str(len(rows)) + ' to update with state name'
    cursor.execute("UPDATE state_{0}_joined_data SET propertystate = '{1}' WHERE propertystate IS NULL" .format(state_number, state))
    conn.commit()

# Get api keys for geocoding
def get_api_keys():
    keys_data = pandas.read_csv('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/google_api_keys.csv',header=0, delimiter=',')
    places_key = keys_data.iloc[0]['Places API key']
    maps_key = keys_data.iloc[0]['Maps API key']

    return{'places_key': places_key, 'maps_key': maps_key}

# Geocode rows that have null latitude
def geocode_data(state_number):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    delete_rows(state_number)

    update_state_field_if_null(state_number)

    keys = get_api_keys()
    places_key = keys['places_key']
    maps_key = keys['maps_key']

    # get the rows where latitude field is null, then geocode
    cursor.execute("SELECT * FROM state_{0}_joined_data WHERE propertyaddresslatitude IS NULL" .format(state_number))
    all_rows = cursor.fetchall()

    print 'There are: ' + str(len(all_rows)) + ' rows to update'

    for row in all_rows:
        importparcelid = row[2]
        street_address = row[3].replace(' ','+')
        state = row[5]
        if str(row[4]) == 'None':
            address_query = str(street_address + '+' + state)
        else:
            city = row[4].replace(' ', '+')
            address_query = str(street_address + '+' + city+ '+' + state)
        url = 'https://maps.googleapis.com/maps/api/geocode/json?address=' + address_query + '&key=' + maps_key

        # call api and parse response
        response = urllib.urlopen(url)
        json_response = json.loads(response.read())
        if json_response['results'] == []:
            print 'This location does not exist'
        else:
            geocoded_lat = json_response['results'][0]['geometry']['location']['lat']
            geocoded_long = json_response['results'][0]['geometry']['location']['lng']
            full_address = json_response['results'][0]['formatted_address']
            print 'Lat is: ' + str(geocoded_lat) + ' Long is: ' + str(geocoded_long)
            cursor.execute("UPDATE state_%s_joined_data SET propertyaddresslatitude = '%s', propertyaddresslongitude = '%s' "
                           "WHERE importparcelid = %s" % (state_number, geocoded_lat, geocoded_long, importparcelid))
            cursor.execute("UPDATE state_%s_joined_data SET geocoded = 'g'"
                           "WHERE importparcelid = %s; " % (state_number, importparcelid))
            conn.commit()
            print 'Updated lat and long'

    # delete rows that still have no latitude
    cursor.execute("DELETE FROM state_{0}_joined_data WHERE propertyaddresslatitude IS NULL; " .format(state_number))
    conn.commit()

    print 'Deleted rows'

# This method was used for one or two states for which the geocoded column wasn't created originally and is no longer part of the workflow
def add_and_populate_geocoded_column(state_number):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    asmt_table = 'state_{0}_zasmt_main' .format(state_number)
    joined_data_to_update = 'state_{0}_joined_data' .format(state_number)
    cursor.execute("ALTER TABLE %s "
                   "ADD geocoded TEXT;" % (joined_data_to_update))
    conn.commit()
    cursor.execute("SELECT * FROM %s " % (asmt_table))
    all_rows = cursor.fetchall()

    for row in all_rows:
        importparcelid_asmt = row[2]
        propertyaddresslatitude_asmt = row[10]

        if propertyaddresslatitude_asmt is None:
            cursor.execute("UPDATE state_%s_joined_data SET geocoded = 'g'"
                   "WHERE importparcelid = %s; " % (state_number, importparcelid_asmt))
            conn.commit()
            print 'Updated row'

# This little method can be altered to run any query. It is not part of the main workflow
def run_sql_query():
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    cursor.execute("ALTER TABLE state_6_joined_data_2 ADD geocoded TEXT")
    conn.commit()
    print 'ran query'

# If the db gets corrupted or hung up in some way, this method may help fix it
def vaccuum_anaylze_db(state_number):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    old_isolation_level = conn.isolation_level
    conn.set_isolation_level(0)
    cursor.execute("VACUUM ANALYZE state_{0}_joined_data" .format(state_number))
    conn.set_isolation_level(old_isolation_level)

# This is the master method to run all others.
def clean_data(state_number):
    create_tables(state_number)
    read_zasmt_main_csv_data_and_insert_rows(state_number)
    read_ztrans_main_csv_data_and_insert_rows(state_number)
    read_ztrans_propinfo_csv_data_and_insert_rows(state_number)
    read_zasmt_value_csv_data_and_insert_rows(state_number)
    read_zasmt_building_csv_data_and_insert_rows(state_number)
    read_zasmt_buildingareas_csv_data_and_insert_rows(state_number)
    join_data(state_number)
    delete_rows(state_number)
    update_state_field_if_null(state_number)
    geocode_data(state_number)

# clean_data(12)
delete_dupes_from_joined_data([6])
#run_sql_query()





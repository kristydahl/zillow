import pandas
import csv
import urllib
import json
import psycopg2
import gc
import codecs
from datetime import datetime

def connect_to_db():
    conn_string = "host='localhost' dbname='test_zillow' user='kristinadahl' password='latte4me'"

    # print the connection string we will use to connect
    print "Connecting to database\n	->%s" % (conn_string)

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_string)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()
    print "Connected!\n"
    return conn_string

def create_tables(state_number):
    conn_string = connect_to_db()

    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # This chunk creates one table
    # command = ("""CREATE TABLE state_{0}_ztrans_propinfo (
    #                 transid TEXT NOT NULL,
    #                 importparcelid BIGINT)
    #                 WITH OIDS """ .format(state_number))
    commands = (
        """
        CREATE TABLE state_{0}_ztrans_main (
            trans_id TEXT PRIMARY KEY NOT NULL,
            fips TEXT NOT NULL,
            loanratetypestndcode TEXT,
            loanduedate TEXT
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_ztrans_propinfo (
            transid TEXT NOT NULL,
            importparcelid BIGINT NOT NULL
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_zasmt_buildingareas (
            rowid TEXT NOT NULL,
            buildingareasqft BIGINT,
            fips TEXT
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_zasmt_main (
            rowid TEXT NOT NULL,
            importparcelid BIGINT PRIMARY KEY NOT NULL,
            fips TEXT NOT NULL,
            propertyfullstreetaddress TEXT,
            propertycity TEXT,
            propertystate TEXT,
            propertyzip TEXT,
            loadid BIGINT,
            propertyaddresslatitude TEXT,
            propertyaddresslongitude TEXT,
            taxamount NUMERIC
            loadid BIGINT
        ) WITH OIDS
        """ .format(state_number),
        """
        CREATE TABLE state_{0}_zasmt_value (
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

    #For one table:
    # cursor.execute(command)
    # cursor.close
    # conn.commit()

    for command in commands:
        print command
        cursor.execute(command)
        cursor.close
        conn.commit()


def check_if_field_is_null(field):
        if field in['', '\x00',' ']:
            return None
        else:
            return field

# WORKING!
def read_ztrans_main_csv_data_and_insert_rows(filename):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

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

            field = LoanRateTypeStndCode
            LoanRateTypeStndCode = check_if_field_is_null(field)

            field = LoanDueDate
            LoanDueDate = check_if_field_is_null(field)

            if FIPS in list_of_fips_codes:
                cursor.execute("INSERT INTO state_11_ztrans_main (trans_id, fips, loanratetypestndcode, loanduedate) VALUES (%s, %s, %s, %s)", (TransId, FIPS, LoanRateTypeStndCode, LoanDueDate))
                conn.commit()

    print 'Ztrans main done'
# WORKING!
def read_ztrans_propinfo_csv_data_and_insert_rows(filename):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    print 'Ztrans propinfo starting'
    # read ztrans_propinfo csv file and insert rows into corresponding table
    with open(filename) as infile:
        for index, row in enumerate(infile):
            split_row = row.split('|')
            TransId = int(split_row[0])
            ImportParcelId = split_row[64]

            field = ImportParcelId
            ImportParcelId = check_if_field_is_null(field)

            cursor.execute(
                "INSERT INTO state_11_ztrans_propinfo (transid, importparcelid) VALUES (%s, %s)",
                (TransId, ImportParcelId))
            conn.commit()

    print 'Ztrans propinfo done'

# WORKING!
def read_zasmt_main_csv_data_and_insert_rows(filename):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

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
            PropertyFullStreetAddress= split_row[26]
            PropertyCity = split_row[27]
            PropertyState = split_row[28]
            PropertyZip = split_row[29]
            PropertyAddressLatitude = split_row[81]
            PropertyAddressLongitude = split_row[82]
            TaxAmount = split_row[38]
            LoadId = split_row[76]

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

            if FIPS in list_of_fips_codes:
                cursor.execute("INSERT INTO state_11_zasmt_main (rowid, importparcelid, fips, propertyfullstreetaddress, propertycity, propertystate, propertyzip, propertyaddresslatitude, propertyaddresslongitude, taxamount, loadid"
                               ") "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (RowId, ImportParcelId, FIPS, PropertyFullStreetAddress, PropertyCity, PropertyState, PropertyZip, PropertyAddressLatitude, PropertyAddressLongitude, TaxAmount, LoadId))
                conn.commit()

    print 'Zasmt main done'
# WORKING!
def read_zasmt_value_csv_data_and_insert_rows(filename):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

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
                cursor.execute("INSERT INTO state_11_zasmt_value (rowid, totalassessedvalue, assessmentyear, totalmarketvalue, marketvalueyear, totalappraisalvalue, appraisalvalueyear, fips"
                               ") "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (RowId, TotalAssessedValue, AssessmentYear, TotalMarketValue, MarketValueYear, TotalAppraisalValue, AppraisalValueYear, FIPS))
                conn.commit()
    print 'Zasmt value done'

def read_zasmt_building_csv_data_and_insert_rows(filename):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

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
                cursor.execute("INSERT INTO state_11_zasmt_building (rowid, noofunits, buildingconditionstndcode, foundationtypestndcode, totalbedrooms, propertylandusestndcode, yearbuilt, effectiveyearbuilt, fips"
                               ") "
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", (RowId, NoOfUnits, BuildingConditionStndCode, FoundtationTypeStndCode, TotalBedrooms, PropertyLandUseStndCode, YearBuilt, EffectiveYearBuilt, FIPS))
                conn.commit()
    print 'Zasmt building done'

def read_zasmt_buildingareas_csv_data_and_insert_rows(filename):
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # get list of fips codes
    df_of_fips_codes = pandas.read_csv('/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/coastal_counties_fips_name.csv', sep=',', header=0,dtype='str')
    list_of_fips_codes = df_of_fips_codes['FIPS'].values.tolist()

    print 'Zasmt buildingareas starting'
    # read asmt_value csv file and insert rows into corresponding table
    with open(filename) as infile:
        for index, row in enumerate(infile):
            split_row = row.split('|')
            RowId = split_row[0]
            BuildingAreaSqFt = split_row[4]
            FIPS = split_row[5]

            field = BuildingAreaSqFt
            BuildingAreaSqFt = check_if_field_is_null(field)

            if FIPS in list_of_fips_codes:
                cursor.execute("INSERT INTO state_11_zasmt_buildingareas (rowid, buildingareasqft, fips"
                               ") "
                               "VALUES (%s, %s, %s)", (RowId, BuildingAreaSqFt, FIPS))
                conn.commit()
    print 'Zasmt building areas done'

def join_data():
    # connect to db
    conn_string = connect_to_db()
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # how can i best clean this up? will need to pass in state number for sure...how else?
    cursor.execute("SELECT state_11_ztrans_main.trans_id, state_11_ztrans_main.fips, state_11_ztrans_main.loanratetypestndcode, state_11_ztrans_main.loanduedate, "
                   "state_11_ztrans_propinfo.transid, state_11_ztrans_propinfo.importparcelid, "
                   " state_11_zasmt_main.rowid, state_11_zasmt_main.importparcelid, state_11_zasmt_main.propertyfullstreetaddress, state_11_zasmt_main.propertycity, state_11_zasmt_main.propertystate, "
                   "state_11_zasmt_main.propertyzip, state_11_zasmt_main.propertyaddresslatitude, state_11_zasmt_main.propertyaddresslongitude, state_11_zasmt_main.taxamount,"
                   "state_11_zasmt_value.rowid, state_11_zasmt_value.totalassessedvalue, state_11_zasmt_value.assessmentyear, state_11_zasmt_value.totalmarketvalue, state_11_zasmt_value.marketvalueyear,"
                   "state_11_zasmt_value.totalappraisalvalue, state_11_zasmt_value.appraisalvalueyear,"
                   "state_11_zasmt_building.rowid, state_11_zasmt_building.noofunits, state_11_zasmt_building.buildingconditionstndcode, state_11_zasmt_building.foundationtypestndcode, state_11_zasmt_building.totalbedrooms,"
                   "state_11_zasmt_building.propertylandusestndcode, state_11_zasmt_building.yearbuilt, state_11_zasmt_building.effectiveyearbuilt,"
                   "state_11_zasmt_buildingareas.rowid, state_11_zasmt_buildingareas.buildingareasqft "
                   "FROM state_11_ztrans_main INNER JOIN state_11_ztrans_propinfo on state_11_ztrans_main.trans_id = state_11_ztrans_propinfo.transid "
                   "INNER JOIN state_11_zasmt_main on state_11_zasmt_main.importparcelid = state_11_ztrans_propinfo.importparcelid "
                   "INNER JOIN state_11_zasmt_value on state_11_zasmt_value.rowid = state_11_zasmt_main.rowid "
                   "INNER JOIN state_11_zasmt_building on state_11_zasmt_building.rowid=state_11_zasmt_main.rowid "
                   "INNER JOIN state_11_zasmt_buildingareas on state_11_zasmt_buildingareas.rowid=state_11_zasmt_main.rowid;")

    head_rows = cursor.fetchmany(size=2)
    x = cursor.fetchall()
    print head_rows

join_data()


#create_tables('11')
#read_ztrans_main_csv_data_and_insert_rows('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/11/11/ZTrans/Main.txt')
#read_ztrans_propinfo_csv_data_and_insert_rows('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/11/11/ZTrans/PropertyInfo.txt')
# read_zasmt_main_csv_data_and_insert_rows('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/11/11/ZAsmt/Main.txt')
# read_zasmt_value_csv_data_and_insert_rows('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/11/11/ZAsmt/Value.txt')
# read_zasmt_building_csv_data_and_insert_rows('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/11/11/ZAsmt/Building.txt')
# read_zasmt_buildingareas_csv_data_and_insert_rows('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/11/11/ZAsmt/BuildingAreas.txt')

#read_in_csv_data_in_chunks_and_insert_rows('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/06/ZTrans/Main.txt')
#use_csv_pkg('/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/06/ZTrans/Main.txt')
#create_tables('11')











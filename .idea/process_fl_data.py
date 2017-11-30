import pandas
import csv
import urllib
import json
import psycopg2
import gc
import codecs
from datetime import datetime

# create tables
cursor.execute("""CREATE TABLE state_12_pre_geocode_from_csv(
    loadid
BIGINT
NOT
NULL,
importparcelid
BIGINT,
propertyfullstreetaddress
TEXT,
propertycity
TEXT,
propertystate
TEXT,
propertyzip
TEXT,
propertyhousenumber
TEXT,
propertyaddresslatitude
TEXT,
propertyaddresslongitude
TEXT,
taxamount
NUMERIC,
created
TEXT,
updated
TEXT,
totalassessedvalue
NUMERIC,
assessmentyear
TEXT,
totalmarketvalue
NUMERIC,
marketvalueyear
TEXT,
totalappraisalvalue
NUMERIC,
appraisalvalueyear
TEXT,
noofunits
INT,
buildingconditionstndcode
TEXT,
foundationtypestndcode
TEXT,
totalbedrooms
INT,
propertylandusestndcode
TEXT,
yearbuilt
TEXT,
effectiveyearbuilt
TEXT,
buildingareasqft
BIGINT,
buildingareastndcode
TEXT,
propertyaddressunitdesignator
TEXT,
propertyaddressunitnumber
TEXT,
loanratetypestndcode
TEXT,
loanduedate
TEXT,
propertysequencenumber
INT,
propertyaddresscensustractandblock
TEXT,
geocoded
TEXT) WITH
OIDS;""")

cursor.execute("""CREATE
TABLE
state_12_geocoded_geocodio_from_csv(
    id
SERIAL,
rowid
TEXT
NOT
NULL,
importparcelid
BIGINT
NOT
NULL,
loadid
TEXT
NOT
NULL,
address
TEXT,
city
TEXT,
state
TEXT,
zip_code
TEXT,
latitude
TEXT,
longitude
TEXT,
accuracy_score
NUMERIC,
accuracy_type
TEXT,
number
TEXT,
street
TEXT,
citygeocodio
TEXT,
stategeocodio
TEXT,
county
TEXT,
zipgeocodio
TEXT,
country
TEXT
) WITH
OIDS;""")

# copy csv files into tables (do the same for pre_geocode using file-specific fields
cursor.execute("""COPY
state_12_geocoded_geocodio_from_csv(rowid,
                                    importparcelid,
                                    loadid,
                                    address,
                                    city,
                                    state,
                                    zip_code,
                                    latitude,
                                    longitude,
                                    accuracy_score,
                                    accuracy_type,
                                    number,
                                    street,
                                    citygeocodio,
                                    stategeocodio,
                                    county,
                                    zipgeocodio,
                                    country)
FROM
'/Users/kristinadahl/Desktop/Union of Concerned Scientists/coastal_work/permanent_inundation/zillow/original_data/12/state_12_joined_data_geocoded_geocodio.csv'
DELIMITER
','
CSV
HEADER;""")

# Delete dupes from pre_geocode:
cursor.execute("""CREATE TABLE state_12_pg_test_nodupes AS SELECT * FROM (SELECT row_number() OVER(PARTITION BY importparcelid, loadid ORDER BY buildingareasqft desc)
AS rn, * FROM state_12_pre_geocode_from_csv) AS SubQueryAlias WHERE rn = 1;""")

#Delete dupes from geocoded_geocodio:
cursor.execute("""CREATE TABLE state_12_gg_test_nodupes AS SELECT * FROM(SELECT row_number() OVER(PARTITION BY rowid, importparcelid, loadid) AS rn, *
FROM state_12_geocoded_geocodio_from_csv) AS SubQueryAlias WHERE rn = 1;""")


#join pg test no dupes and gg test no dupes by importparcelid
cursor.execute("""CREATE
TABLE
state_12_joined_data_geocoded_geocodio_all_fields
AS
SELECT
state_12_gg_test_nodupes.rowid,
state_12_gg_test_nodupes.importparcelid,
state_12_gg_test_nodupes.loadid,
state_12_gg_test_nodupes.latitude,
state_12_gg_test_nodupes.longitude,
state_12_gg_test_nodupes.accuracy_score,
state_12_gg_test_nodupes.accuracy_type,
state_12_pg_test_nodupes.id,
state_12_pg_test_nodupes.propertyfullstreetaddress,
state_12_pg_test_nodupes.propertycity,
state_12_pg_test_nodupes.propertystate,
state_12_pg_test_nodupes.propertyzip,
state_12_pg_test_nodupes.propertyhousenumber,
state_12_pg_test_nodupes.propertyaddresslatitude,
state_12_pg_test_nodupes.propertyaddresslongitude,
state_12_pg_test_nodupes.taxamount,
state_12_pg_test_nodupes.created,
state_12_pg_test_nodupes.updated,
state_12_pg_test_nodupes.totalassessedvalue,
state_12_pg_test_nodupes.assessmentyear,
state_12_pg_test_nodupes.totalmarketvalue,
state_12_pg_test_nodupes.marketvalueyear,
state_12_pg_test_nodupes.totalappraisalvalue,
state_12_pg_test_nodupes.appraisalvalueyear,
state_12_pg_test_nodupes.noofunits,
state_12_pg_test_nodupes.buildingconditionstndcode,
state_12_pg_test_nodupes.foundationtypestndcode,
state_12_pg_test_nodupes.totalbedrooms,
state_12_pg_test_nodupes.propertylandusestndcode,
state_12_pg_test_nodupes.yearbuilt,
state_12_pg_test_nodupes.effectiveyearbuilt,
state_12_pg_test_nodupes.buildingareasqft,
state_12_pg_test_nodupes.buildingareastndcode,
state_12_pg_test_nodupes.propertyaddressunitdesignator,
state_12_pg_test_nodupes.propertyaddressunitnumber,
state_12_pg_test_nodupes.loanratetypestndcode,
state_12_pg_test_nodupes.loanduedate,
state_12_pg_test_nodupes.propertysequencenumber,
state_12_pg_test_nodupes.propertyaddresscensustractandblock,
state_12_pg_test_nodupes.geocoded
FROM
state_12_gg_test_nodupes
LEFT
JOIN
state_12_pg_test_nodupes
ON
state_12_gg_test_nodupes.importparcelid = state_12_pg_test_nodupes.importparcelid;""")
import pandas
import csv


def join_ztrax_tables():

    # specify folder where zillow data layouts live

    data_folder = '/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/layouts/'

    # these layout files should live in a higher folder than the state-level data. move this up outside of the loop, move files, and fix folder paths
    ztrans_main_layout = pandas.read_csv(str(ztrans_folder + 'ztrans_main_layout.csv'), sep='\t', header=None)
    ztrans_propinfo_layout = pandas.read_csv(ztrans_folder + 'ztrans_propinfo_layout.csv', sep='\t', header=None)

    zasmt_main_layout = pandas.read_csv(zasmt_folder + 'zasmt_main_layout.csv', sep='\t', header=None)
    zasmt_value_layout = pandas.read_csv(zasmt_folder + 'zasmt_value_layout.csv', sep='\t', header=None)
    zasmt_building_layout = pandas.read_csv(zasmt_folder + 'zasmt_value_layout.csv', sep='\t', header=None)
    # specify overall data folder

    for state in state_numbers:

        # specify folder where state-specific data lives
        data_folder = '/Users/kristinadahl/Dropbox/zillow_data_on_dropbox/{0}/' .format(state) # state could be a parameter passed by method so you could loop through states

        # specify subfolders and file names
        ztrans_folder = data_folder + 'Ztrans/'
        zasmt_folder = data_folder + 'Zasmt/'

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

        ztrans_main_all = pandas.read_table(ztrans_main_file,header=None,sep='|',names=ztrans_main_colnames)
        ztrans_main = ztrans_main_all['TransId']
        ztrans_propinfo_all = pandas.read_table(ztrans_propinfo_file,header=None,sep='|',names=ztrans_propinfo_colnames)
        ztrans_propinfo = ztrans_propinfo_all[['TransId','ImportParcelID']]
        zasmt_main_all = pandas.read_table(zasmt_main_file,header=None,sep='|',names=zasmt_folder_main_colnames)
        zasmt_main = zasmt_main_all[['RowID','ImportParcelID','FIPS','PropertyFullStreetAddress','PropertyCity','PropertyState','PropertyZip','LoadID','PropertyAddressLatitude','PropertyAddressLongitude']] # is this all I need?
        zasmt_value_all = pandas.read_table(zasmt_value_file,header=None,sep='|',names=zasmt_value_colnames)
        zasmt_value = zasmt_value_all[['RowID','TotalAssessedValue','AssessmentYear','TotalMarketValue','MarketValueYear','TotalAppraisalValue','AppraisalValueYear']] # is this all I need?
        zasmt_building_all = pandas.read_table(zasmt_building_file,header=None,sep='|',names=zasmt_building_colnames)
        zasmt_building = zasmt_building_all[['RowID','NoOfUnits','BuildingConditionStndCode','TotalBedrooms']]

        # join ztrans_main to ztrans_propinfo by TransID

        join_ztrans_main_ztrans_propinfo = pandas.merge(ztrans_main, ztrans_propinfo,on='TransId')

        # join asmt_main to joined file above

        join_ztrans_main_ztrans_propinfo_zasmt_main = pandas.merge(join_ztrans_main_ztrans_propinfo, zasmt_main, left_on='TransId',right_on='ImportParcelID')

        # join asmt_value to joined file above

        join_ztrans_main_ztrans_propinfo_zasmt_main_zasmt_value = pandas.merge(join_ztrans_main_ztrans_propinfo_zasmt_main, zasmt_value, on='RowID')

        # join asmt_building to joined file above
        join_all = pandas.merge(join_ztrans_main_ztrans_propinfo_zasmt_main_zasmt_value, zasmt_building, on='RowID')














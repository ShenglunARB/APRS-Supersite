"""
This source code is used to process raw data from Picarro monitor. 

Picarro monitors: 
- Model: G2401, CO2-CO-CH4-H2O analyzer 
- Model: G2307, HCHO analyzer
- Model: G2103, NH3-H2O analyzer

@auther: Shenglun Wu
@date: Dec 2024
"""

import numpy as np # type: ignore
import pandas as pd # type: ignore
import sys, os


def standardize_name(analyzer):
    """
    This function standarize the name of species and analyzer.
    Input:
        analyzer: str, species name (e.g. 'COC', 'Formaldehyde, 'Ammonia')
    Return:
        analyzer: str, standardized analyzer name
        species: str, standardized species name
    """

    if analyzer == 'G2401' or analyzer == 'CO' or analyzer == 'CO2' \
        or analyzer == 'CH4' or analyzer == 'CO_Picarro':
        analyzer = 'CO_Picarro'
        species = 'CO2-CO-CH4'

    elif analyzer == 'Formaldehyde' or analyzer == 'HCHO' or analyzer == 'G2307' \
        or analyzer == 'HCHO_Picarro':
        analyzer = 'HCHO_Picarro'
        species = 'HCHO'

    elif analyzer == 'Ammonia' or analyzer == 'NH3' or analyzer == 'G2103' \
        or analyzer == 'NH3_Picarro':
        analyzer = 'NH3_Picarro'
        species = 'NH3-H2O'

    else:
        print('Monitor not found!')
        sys.exit()

    return analyzer, species


def find_data_folder(site, analyzer):
    """ 
    Returns the folder location based on the site and analyzer name. 
    Input: 
        site (str): Name of the site (e.g., 'Fresno' or 'Bakersfield') 
        analyzer (str): Name of the analyzer (e.g., 'NH3', 'CO', 'HCHO') 
    Returns: 
        folder: str The folder location that stores the data 
    """ 
    
    base_path = "C:/Users/swu/OneDrive - California Air Resources Board/Shared Documents - RD ASCSB APRS/General/02. In-House Research_Needs cleaning/!Site Operations" 

    valid_sites = ['Fresno-Garland Supersite', 'Berkersfield-CA Supersite', 'MWO'] 

    if site not in valid_sites: 
        raise ValueError(f"Invalid site name '{site}'. Valid options are: {valid_sites}")

    valid_analyzers = ['NH3_Picarro', 'CO_Picarro', 'HCHO_Picarro']         
    
    if analyzer not in valid_analyzers: 
        raise ValueError(f"Invalid analyzer name '{analyzer}'. Valid options are: {valid_analyzers}") 

    folder = f"{base_path}/{site}/{analyzer}" 

    return folder  # Example usage site = "Fresno" analyzer = "NH3" 


def find_daily_raw_datafile(folder, date):
    """
    This function is used to find daily raw datafiles from Picarro monitor in Shared folder Data Portal.
    Input: 
        folder: str, folder path to the project and analyzer. 
        date: datetime, date to process (in UTC, set by the monitor)
    Output:
        filepath: list, list of path of raw datafiles in one day
    """
    
    # formatting Date and species
    date = pd.to_datetime(date)
    YY = date.strftime('%Y')
    mm = date.strftime('%m')
    dd = date.strftime('%d')
    
    # get all files in the directory when folder and data file exist
    folderpath = folder + '/Level0_Raw_Data/' + YY + '/' + mm + '/' + dd
    
    if not os.path.exists(folderpath):
        print('Folder not found on ', date.strftime('%Y%m%d'), '!')
        pass

    elif len(os.listdir(folderpath)) == 0:
        print('No data file on ', date.strftime('%Y%m%d'), '!')
        pass

    else:
        files = os.listdir(folderpath)
        filepath = [folderpath + '/' + file for file in files]

        # get data from 1 day before (part of data was recorded on the previous day's data)
        date2 = date - pd.DateOffset(days=1)
        YYmm2 = date2.strftime('%Y%m')
        dd2 = date2.strftime('%d')
        folderpath2 = folder + '/Level0_Raw_Data/' + YYmm2 + '/' + dd2

        # check if the folderpath2 exists
        if not os.path.exists(folderpath2):
            pass
        else:
            files2 = os.listdir(folderpath2)
            files2 = [file for file in files2 if '-23' in file]  # select string contains -23 in files2 (data recorded from 23:00)
            filepath2 = [folderpath2 + '/' + file for file in files2]

            # combine filepaths
            filepath = filepath2 + filepath

        return filepath


# Degugging step1: check filename (make sure the file is from the correct analyzer) 
def check_data_filename(filepath, analyzer):
    """
    Make sure a data file is from the correct analyzer (first 10 digit of the raw data file is the serial number of the Picarro analyzer, check whether the filepath contains correction Site location and Analyzer name (e.g. Fresno/CO_Picarro) that matches the serial number)
    Input:
        filepath: list, list of path of raw datafiles in one day
        analyzer: str, standard analyzer name (shown in id_dict)
    Output:
        warning message will be printed out if there is any trouble
    """

    # analyzer and its id number
    id_dict = {'CO_Picarro': 'CFKADS', 'NH3_Picarro': 'AHDS', 'HCHO_Picarro': 'LBDS'}

    for file in filepath:
        analyzer_id = id_dict[analyzer]

        if analyzer_id not in file:
            date = filepath[-1].split('/')[-4:-1]
            date = ''.join(date)
            print('Wrong ', analyzer, ' data files on ' , date, '!')            
            
            break # only report warning once if there is any trouble


def daily_raw_data(filepath):
    """
    This function is to get daily raw data in one dataframe.
    Input:
        folder: str, folder path to the project. (e.g 'W:/3. APRS - Data_Portal/5. Fresno-Garland Supersite/HCHO_Picarro')
        date: datetime, date to process
    Output:
        df_daily: dataframe, daily raw data in one dataframe
    """

    # find date of data
    date = pd.to_datetime(filepath[-1].split('/')[-1].split('-')[1], format='%Y%m%d')

    # combine all data files in one day
    df_daily = pd.DataFrame()

    for i in range(len(filepath)):
    
        # open single data file
        df = pd.read_csv(filepath[i], sep=r'\s+')

        # combine all data files
        df_daily = pd.concat([df_daily, df])

    # select data for the date
    df_daily = df_daily[pd.to_datetime(df_daily['DATE']) == date]

    df_daily['DATETIME'] = pd.to_datetime(df_daily['DATE'] + ' ' + df_daily['TIME'])
    df_daily = df_daily.set_index('DATETIME')

    return df_daily


# Degubbing step2: check raw data alarm status (alarms from analyzer)
def screen_warning_df_daily(df_daily, analyzer):
    """
    Check any warnings in the raw daily data directly reported from Picarro analyzer (warnings include: ALARM_STATUS, INST_STATUS, MPVPosition).

    Input:
        df_daily: dataframe, daily raw data in one dataframe

    Output:
        warning message will be printed out if there is any trouble
    """

    if analyzer == 'NH3_Picarro':
        inst_status = 899
    else:
        inst_status = 963

    # check if ALARM_STATUS is all 0 (alarm code)
    if df_daily['ALARM_STATUS'].sum() != 0:
        print('ALARM_STATUS is not all 0!')   
    else:
        pass

    # check INST_STATUS is all 963.0 (instriment status)
    if df_daily['INST_STATUS'].sum() != inst_status * len(df_daily):
        print('INST_STATUS has warning!')
    else:
        pass
    
    # check MPVPosition is all 0 (calve position)
    if df_daily['MPVPosition'].sum() != 0:
        print('MPVPosition is not all 0!')
    else:
        pass


def clean_warning_data(df_daily, analyzer):
    """
    This function is used to filter out the warning data from raw dataset
    Input:
        df_daily: dataframe, daily raw data in one dataframe
    
    Output:
        df_daily_clean: dataframe, daily raw data without warning
    """

    if analyzer == 'NH3_Picarro':
        inst_status = 899
    else:
        inst_status = 963    

    # clean warning data
    df_daily_nowarning = df_daily.copy()
    df_daily_nowarning = df_daily_nowarning[
        (df_daily_nowarning['ALARM_STATUS'] == 0)&
        (df_daily_nowarning['INST_STATUS'] == inst_status)&
        (df_daily_nowarning['MPVPosition'] == 0)
    ]
    
    return df_daily_nowarning


def clean_parameter_column(df_daily_nowarning, analyzer):
    """
    This function is used to filter useful column (measured concentration) from the raw daily data from Picarro monitor.
    Input:
        df_daily: dataframe, daily raw data in one dataframe
    Output:
        df_daily_clean: dataframe, daily raw data with useful columns
    """

    column_dict = {'CO_Picarro': ['DATE', 'TIME', 'CO', 'CO2', 'CO2_dry', 
                                  'CH4', 'CH4_dry', 'H2O'],
                   'HCHO_Picarro': ['DATE', 'TIME', 'H2CO'],
                   'NH3_Picarro': ['DATE', 'TIME', 'NH3', 'NH3_dry']}

    parameter_columns = column_dict[analyzer]
    
    df_daily_clean = df_daily_nowarning.copy()

    # Renaming the columns (no '_sync' for sync data) 
    df_daily_clean.columns = \
        [col.replace('_sync', '') for col in df_daily_clean.columns]

    df_daily_clean = df_daily_clean[parameter_columns]

    # rename columns
    #if analyzer == 'CO_Picarro':
    #    df_daily_clean.rename(
    #        columns={'CO2_dry':'CO2', 'CH4_dry':'CH4', 'H2O':'H2O'}, inplace=True
    #    )
    
    #if analyzer == 'NH3_Picarro':
    #    df_daily_clean.rename(
    #        columns={'NH3_dry':'NH3'}, inplace=True
    #    )

    return df_daily_clean


def average_daily_data(df_daily_clean, average_time, std=True, se=True):
    """
    This function is used to average daily raw data from Picarro monitor, .
    Input:
        df_daily: dataframe, daily raw data in one dataframe
        average_time: str, time to average (e.g. '1min', '10min')
        missing_data: bool, True if fill in missing data, False if not
    Output:
        df_avg: dataframe, averaged data
    """

    # averaging
    df = df_daily_clean.copy()
    df.drop(columns=['DATE', 'TIME'], inplace=True)
    
    # calculate mean, std, and se for each time
    def calc_se(x): 
        return x.std() / np.sqrt(len(x)) 
    # Define the aggregation functions 
    agg_funcs = {col: ['mean', 'std', calc_se] for col in df.columns} 

    # Resample and aggregate 
    df_avg = df.resample(average_time).agg(agg_funcs)
    df_avg = df_avg.round(3) 

    # Flatten the MultiIndex columns 
    new_columns = [] 
    for col in df.columns: 
        new_columns.extend([col, f"{col}_std", f"{col}_se"]) 
    df_avg.columns = new_columns

    return df_avg


def fill_missing_data(df_avg, average_time):
    """
    Used to fill in missing time in a specific time range.
    Input:
        df_avg: dataframe, averaged data
        average_time: str, time to average (e.g. '1min', '10min')
    Output:
        df_avg: dataframe, averaged data with missing time filled
    """
    
    df_avg['TIME'] = df_avg.index.time

    # fill in missing time
    time_range = pd.date_range(start='00:00', end='23:59', freq=average_time).time
    df_dt = pd.DataFrame(
        {'DATE': df_avg.index[0].date().strftime('%Y-%m-%d'),
        'TIME' : time_range}
    )
    df_avg = pd.merge(df_dt, df_avg, on='TIME', how='left')    

    return df_avg


def processed_data_folder_level1a(folder):
    """
    Generate folder path to store processed data.
    Input:
        folder: str, folder path to the analyzer data. From function: find_data_folder
    Output:
        folder: str, folder path to store processed data
    """

    folder_level1a = folder + '/Level1A_Processed_Data_1min'
    if not os.path.exists(folder):
        os.makedirs(folder)

    return folder_level1a


def processed_data_folder_level1b(folder):
    """
    Generate folder path to store processed data.
    Input:
        folder: str, folder path to the analyzer data. From function: find_data_folder
    Output:
        folder: str, folder path to store processed data
    """

    folder_level1b = folder + '/Level1B_Processed_Data_1hr'
    if not os.path.exists(folder):
        os.makedirs(folder)

    return folder_level1b


def store_processed_data(folder_level1a, site, species, date, df_avg):
    """
    Store processed data to the folder.
    Input:
        folder_level1a: str, folder path to store processed data
        site: str, site name (e.g. 'Fresno-Garland Supersite')
        analyzer: str, standard species name
        date: datetime, date to process
        df_avg: dataframe, processed averaged df_daily
    Output:
        None
    """

    # create file name
    site_name = site.split('-')[0]
    monitor_name = 'Picarro-' + species
    YY = pd.to_datetime(date).strftime('%Y')
    date = pd.to_datetime(date).strftime('%Y%m%d')

    filename = site_name + '_' + monitor_name + '_' + date + '.csv'
    filepath = folder_level1a + '/' + YY + '/' + filename

    # create folder if not exist
    if not os.path.exists(folder_level1a + '/' + YY):
        os.makedirs(folder_level1a + '/' + YY)

    # rename columns
    df_avg.rename(columns={'DATE':'DATE_UTC', 'TIME':'TIME_UTC'}, inplace=True)

    # fill NaN with -9999
    df_avg.fillna(-9999, inplace=True)

    df_avg.to_csv(filepath, index=False)
    print('Data is stored: ', filename)


def main(site, analyzer, date, average_time='1min'):
    """
    Main function to process raw data from Picarro monitor.
    Input:
        site: str, site name (e.g. 'Fresno-Garland Supersite')
        analyzer: str, analyzer name (e.g. 'NH3', 'CO', 'HCHO')
        date: datetime, date to process
        average_time: str, time to average (e.g. '1min', '10min')
    Output:
        None
    """

    analyzer, species = standardize_name(analyzer)

    # find folder path
    folder = find_data_folder(site, analyzer)

    # find daily raw datafile
    filepath = find_daily_raw_datafile(folder, date)

    if filepath is not None:
        
        # check data filename
        check_data_filename(filepath, analyzer)

        # get daily raw data
        df_daily = daily_raw_data(filepath)

        # check raw data alarm status
        screen_warning_df_daily(df_daily, analyzer)

        # clean warning data
        df_daily_nowarning = clean_warning_data(df_daily, analyzer)

        if df_daily_nowarning.empty:
            print('All data has warning on ', date, '!')
            pass

        else:
            # clean parameter column
            df_daily_clean = clean_parameter_column(df_daily_nowarning, analyzer)

            # average daily data
            df_avg = average_daily_data(df_daily_clean, average_time, std=True, se=True)
            df_avg = fill_missing_data(df_avg, average_time)
            
            # store processed data
            if average_time == '1min':
                folder_level1a = processed_data_folder_level1a(folder)
                store_processed_data(folder_level1a, site, species, date, df_avg)
            
            elif average_time == '60min':
                folder_level1b = processed_data_folder_level1b(folder)
                store_processed_data(folder_level1b, site, species, date, df_avg)
            
            else:
                print('Invalid average time!')
                pass


if __name__ == "__main__":
    site = 'Fresno-Garland Supersite' # 'Fresno-Garland Supersite', 'Berkersfield-CA Supersite', 'MWO'
    analyzer = 'CO' # 'CO', 'HCHO', 'NH3'
    #date = '2024-11-01'
    #main(site, analyzer, date, average_time='1min')

    dates = pd.date_range(start='2023-11-20', end='2024-12-31', freq='D')
    
    for date in dates:
        main(site, analyzer, date, average_time='1min')

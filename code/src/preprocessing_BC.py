"""
This source code is used to process raw data from AE33 Black Carbon (BC) analyzer

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

    if analyzer == 'BC' or analyzer == 'AE33':
        analyzer = 'BC_AE33'
        species = 'BC'

    else:
        print('Monitor not found!')
        sys.exit()

    return analyzer, species


def find_data_folder(site, analyzer):
    """ 
    Returns the folder location based on the site and analyzer name. 
    Input: 
        site (str): Name of the site
        analyzer (str): Name of the analyzer
    Returns: 
        folder: str The folder location that stores the data 
    """ 
    
    base_path = "C:/Users/swu/OneDrive - California Air Resources Board/Shared Documents - RD ASCSB APRS/General/02. In-House Research_Needs cleaning/!Site Operations" 

    valid_sites = ['Fresno-Garland Supersite', 'Berkersfield-CA Supersite', 'MWO'] 

    if site not in valid_sites: 
        raise ValueError(f"Invalid site name '{site}'. Valid options are: {valid_sites}")

    valid_analyzers = ['BC_AE33']         
    
    if analyzer not in valid_analyzers: 
        raise ValueError(f"Invalid analyzer name '{analyzer}'. Valid options are: {valid_analyzers}") 

    folder = f"{base_path}/{site}/{analyzer}" 

    return folder 


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
    folderpath = folder + '/Level0_Raw_Data/' + YY

    # find file in the folder
    filelsit = os.listdir(folderpath)
    file_date = [file for file in filelsit if 'AE33_AE33-' in file and YY+mm+dd in file]

    if file_date == []:
        print('No data file on ', YY+mm+dd, '!')
        pass

    elif len(file_date) > 1:
        print('Multiple data files on ', YY+mm+dd, '!')
        pass

    else:
        filepath = folderpath + '/' + file_date[0]
        
        return filepath
    


def daily_raw_data(filepath, date):
    """
    This function is to get daily raw data in one dataframe.
    Input:
        folder: str, folder path to the project.
        date: datetime, date to process
    Output:
        df_daily: dataframe, daily raw data in one dataframe
    """

    # get header line
    with open(filepath, 'r') as f: 
        header_line = [f.readline().strip() for _ in range(6)][-1].split('; ')

    df = pd.read_csv(filepath, skiprows=6, header=None, sep=' ')
    df_daily = df.iloc[:,0:len(header_line)]
    df_daily.columns = header_line

    # rename Date and Time columns
    df_daily.rename(
        columns={'Date(yyyy/MM/dd)':'DATE', 'Time(hh:mm:ss)':'TIME'}, inplace=True
    )

    df_daily['DATETIME'] = pd.to_datetime(df_daily['DATE'] + ' ' + df_daily['TIME'])
    df_daily = df_daily.set_index('DATETIME')

    # filter data by date
    df_daily = df_daily[pd.to_datetime(df_daily['DATE']) == pd.to_datetime(date)]

    return df_daily



def screen_warning_df_daily(df_daily):
    """
    Check any warnings in the raw daily data directly reported from AE33 analyzer (warnings include: ).

    Input:
        df_daily: dataframe, daily raw data in one dataframe

    Output:
        warning message will be printed out if there is any trouble
    """


    # check if ALARM_STATUS is all 0 (alarm code)
    if df_daily['Status'].sum() != 0:
        print('Status is not all 0!')   
    else:
        pass

    # check whether any FlowC is out of the range 5 ± 5%
    flowc = 5000
    range = 0.05

    if not df_daily['FlowC'].between(flowc*(1-range), flowc*(1+range)).all():
        print('FlowC is out of the range 5 ± 5%!')
    else:
        pass


def clean_warning_data(df_daily):
    """
    This function is used to filter out the warning data from raw dataset
    Input:
        df_daily: dataframe, daily raw data in one dataframe
    
    Output:
        df_daily_clean: dataframe, daily raw data without warning
    """

    flowc = 5000
    range = 0.05

    df_daily_nowarning = df_daily.copy()
    df_daily_nowarning = df_daily_nowarning[
        (df_daily_nowarning['Status'] == 0)&
        (df_daily_nowarning['FlowC'].between(flowc*(1-range), flowc*(1+range)))
    ]

    # clean negative BCX values
    parameter = ['BC1','BC2','BC3','BC4','BC5','BC6', 'BC7']
    for p in parameter:
        df_daily_nowarning.loc[df_daily_nowarning[p] < -100, p] = np.nan

    return df_daily_nowarning



def clean_parameter_column(df_daily_nowarning):
    """
    This function is used to filter useful column (measured concentration) from the raw daily data from AE33 monitor.
    Input:
        df_daily: dataframe, daily raw data in one dataframe
    Output:
        df_daily_clean: dataframe, daily raw data with useful columns
    """

    parameter_columns = ['DATE', 'TIME',
                         'BC1','BC2','BC3','BC4','BC5','BC6', 'BC7']
    
    df_daily_clean = df_daily_nowarning.copy()
    df_daily_clean = df_daily_clean[parameter_columns]

    return df_daily_clean


def fill_missing_data(df):
    """
    Used to fill in missing time in the resolution '1min'.
    Input:
        df: dataframe
    Output:
        df: dataframe, data with missing time filled
    """

    
    df['TIME'] = df.index.time
    df.drop(columns=['DATE'], inplace=True)
    time_range = '1min'

    # fill in missing time
    time_range = pd.date_range(start='00:00', end='23:59', freq=time_range).time
    df_dt = pd.DataFrame(
        {'DATE': df.index[0].date().strftime('%Y-%m-%d'),
        'TIME' : time_range}
    )
    df = pd.merge(df_dt, df, on=['TIME'], how='left')

    df['DATETIME'] = pd.to_datetime(df['DATE'].astype(str) + ' ' + df['TIME'].astype(str))
    df.set_index('DATETIME', inplace=True)
    
    return df


def average_daily_data(df_daily_clean, average_time, std=True, se=True):
    """
    This function is used to average daily raw data from AE33 monitor.
    Input:
        df_daily: dataframe, daily raw data in one dataframe
        average_time: str, time to average (e.g. , '1hr')
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

    # add DATE and TIME columns
    df_avg['DATE'] = df_avg.index.date
    df_avg['TIME'] = df_avg.index.time
    df_avg = df_avg[['DATE', 'TIME'] + new_columns]

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
    monitor_name = 'AE33-' + species
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
    Main function to process AE33 raw data.
    Input:
        site: str, site name (e.g. 'Fresno-Garland Supersite')
        analyzer: str, standard species name
        date: datetime, date to process
        average_time: str, time to average (e.g. '1min', '60min')
    """

    analyzer, species = standardize_name('BC')
    folder = find_data_folder(site, analyzer)
    filepath = find_daily_raw_datafile(folder, date)

    if filepath is not None:
    
        df_daily = daily_raw_data(filepath, date)
        screen_warning_df_daily(df_daily)
        df_daily_nowarning = clean_warning_data(df_daily)
        
        if df_daily_nowarning.empty:
            print('All data has warning on ', date, '!')
            pass

        else:
            df_daily_clean = clean_parameter_column(df_daily_nowarning)
            df_daily_clean = fill_missing_data(df_daily_clean)

            # process data to 1min resolution
            if average_time == '1min':
                folder_level1a = processed_data_folder_level1a(folder)
                store_processed_data(folder_level1a, site, species, date, df_daily_clean)
            
            # process data to 1hr resolution
            elif average_time == '60min':
                df_avg = average_daily_data(df_daily_clean, average_time)
                folder_level1b = processed_data_folder_level1b(folder)
                store_processed_data(folder_level1b, site, species, date, df_avg)


if __name__ == "__main__":
    site = 'Fresno-Garland Supersite' # 'Fresno-Garland Supersite', 'Berkersfield-CA Supersite', 'MWO'
    analyzer = 'AE33'
    #date = '2024-11-01'
    #main(site, analyzer, date, average_time='1min')

    dates = pd.date_range(start='2023-11-16', end='2023-12-31', freq='D')
    
    for date in dates:
        main(site, analyzer, date, average_time='1min')

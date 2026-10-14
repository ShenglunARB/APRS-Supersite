"""
Preprocess raw data from the Picarro gas analyzers.

Picarro analyzers:
    - Model G2401: CO2 / CO / CH4 / H2O analyzer   (ANALYZER = 'CO')
    - Model G2307: HCHO (formaldehyde) analyzer     (ANALYZER = 'HCHO')
    - Model G2103: NH3 (ammonia) / H2O analyzer      (ANALYZER = 'NH3')

What this script does, for every day between START_DATE and END_DATE:
    1. Finds the raw data files of that day (Level0_Raw_Data/YYYY/MM/DD/).
    2. Prints a warning if the analyzer reported any alarm / status problem.
    3. Removes the rows that have a warning.
    4. Averages the data to 1-minute or 1-hour resolution (mean, std, se).
    5. Saves one CSV file per day:
         1min  -> Level1A_Processed_Data_1min/YYYY/<Site>_Picarro-<Species>_YYYYMMDD.csv
         60min -> Level1B_Processed_Data_1hr/YYYY/<Site>_Picarro-<Species>_YYYYMMDD.csv
       Missing values are written as -9999. All times are UTC.

How to use:
    1. Edit the "USER SETTINGS" block below.
    2. Save this file.
    3. Run it (see README.md), e.g. in the VS Code terminal:
         python preprocessing_Picarro.py

@author: Shenglun Wu
@date: Dec 2024
"""

import os

import numpy as np
import pandas as pd


# =============================================================================
# USER SETTINGS - change these values before running the script
# =============================================================================

# Path to the "!Site Operations" folder of the APRS SharePoint (synced to your
# computer with OneDrive). os.path.expanduser("~") is your home folder
# (C:/Users/<your user name>), so this usually works for everyone without changes.
# If your folder is somewhere else, replace it with the full path, using "/", e.g.
# BASE_PATH = "C:/Users/swu/OneDrive - California Air Resources Board/Shared Documents - RD ASCSB APRS/General/02. In-House Research/!Site Operations"
BASE_PATH = os.path.expanduser("~") + "/OneDrive - California Air Resources Board/Shared Documents - RD ASCSB APRS/General/02. In-House Research/!Site Operations"

# Site to process. Options:
#   'Fresno-Garland Supersite', 'Bakersfield-California Ave Supersite', 'MWO'
SITE = 'MWO'

# Analyzer to process. Options: 'CO', 'HCHO', 'NH3'
ANALYZER = 'CO'

# First and last day to process (UTC), format 'YYYY-MM-DD'. Both days are included.
# To process only one day, use the same date for both.
START_DATE = '2025-02-01'
END_DATE = '2025-03-01'

# Averaging time. Options:
#   '1min'  -> saved in folder Level1A_Processed_Data_1min
#   '60min' -> saved in folder Level1B_Processed_Data_1hr
AVERAGE_TIME = '60min'

# =============================================================================
# END OF USER SETTINGS - you normally do not need to change anything below
# =============================================================================


VALID_SITES = ['Fresno-Garland Supersite', 'Bakersfield-California Ave Supersite', 'MWO']
VALID_AVERAGE_TIMES = ['1min', '60min']


def standardize_name(analyzer):
    """
    Convert any accepted analyzer/species name to the standard names used in
    folder and file names.

    Input:
        analyzer: str, analyzer or species name (e.g. 'CO', 'G2401', 'Formaldehyde', 'Ammonia')
    Return:
        analyzer: str, standard analyzer (folder) name, e.g. 'CO_Picarro'
        species: str, standard species name used in output file names, e.g. 'CO2-CO-CH4'
    """

    if analyzer in ['G2401', 'CO', 'CO2', 'CH4', 'CO_Picarro']:
        analyzer = 'CO_Picarro'
        species = 'CO2-CO-CH4'

    elif analyzer in ['Formaldehyde', 'HCHO', 'G2307', 'HCHO_Picarro']:
        analyzer = 'HCHO_Picarro'
        species = 'HCHO'

    elif analyzer in ['Ammonia', 'NH3', 'G2103', 'NH3_Picarro']:
        analyzer = 'NH3_Picarro'
        species = 'NH3-H2O'

    else:
        raise ValueError(
            f"Analyzer '{analyzer}' not found! Valid options are: 'CO', 'HCHO', 'NH3'"
        )

    return analyzer, species


def find_data_folder(site, analyzer):
    """
    Return the folder that stores the data of one analyzer at one site.

    Input:
        site: str, site name (one of VALID_SITES)
        analyzer: str, standard analyzer name (e.g. 'NH3_Picarro', 'CO_Picarro', 'HCHO_Picarro')
    Return:
        folder: str, e.g. '<BASE_PATH>/MWO/CO_Picarro'
    """

    if site not in VALID_SITES:
        raise ValueError(f"Invalid site name '{site}'. Valid options are: {VALID_SITES}")

    valid_analyzers = ['NH3_Picarro', 'CO_Picarro', 'HCHO_Picarro']

    if analyzer not in valid_analyzers:
        raise ValueError(f"Invalid analyzer name '{analyzer}'. Valid options are: {valid_analyzers}")

    folder = f"{BASE_PATH}/{site}/{analyzer}"

    return folder


def find_daily_raw_datafile(folder, date):
    """
    Find all raw data files of one day.

    The Picarro writes one file per hour, named by the time the file STARTS.
    The file that starts at 23:00 on the previous day also contains the first
    minutes of this day, so it is included as well.

    Input:
        folder: str, analyzer folder (from find_data_folder)
        date: str or datetime, day to process (UTC, the analyzer clock)
    Return:
        filepath: list of str, paths of the raw data files,
                  or None if no file is found (a message is printed)
    """

    date = pd.to_datetime(date)
    YY = date.strftime('%Y')
    mm = date.strftime('%m')
    dd = date.strftime('%d')

    # raw files are stored in Level0_Raw_Data/YYYY/MM/DD/
    folderpath = folder + '/Level0_Raw_Data/' + YY + '/' + mm + '/' + dd

    if not os.path.exists(folderpath):
        print('Folder not found on ', date.strftime('%Y%m%d'), '!')
        return None

    files = os.listdir(folderpath)
    if len(files) == 0:
        print('No data file on ', date.strftime('%Y%m%d'), '!')
        return None

    filepath = [folderpath + '/' + file for file in files]

    # add the 23:00 file(s) from the previous day (they contain data from the first minutes of this day)
    date2 = date - pd.DateOffset(days=1)
    YY2 = date2.strftime('%Y')
    mm2 = date2.strftime('%m')
    dd2 = date2.strftime('%d')
    folderpath2 = folder + '/Level0_Raw_Data/' + YY2 + '/' + mm2 + '/' + dd2

    if os.path.exists(folderpath2):
        files2 = os.listdir(folderpath2)
        files2 = [file for file in files2 if '-23' in file]  # file names with '-23' start at 23:00
        filepath2 = [folderpath2 + '/' + file for file in files2]
        filepath = filepath2 + filepath

    return filepath


# Debugging step 1: check file names (make sure the files are from the correct analyzer)
def check_data_filename(filepath, analyzer):
    """
    Make sure the data files come from the correct analyzer.

    Each raw file name starts with the serial number of the Picarro analyzer
    (e.g. 'CFKADS...' for the CO analyzer). If a file in the folder does not
    match the expected analyzer, a warning is printed (files may have been put
    in the wrong folder).

    Input:
        filepath: list of str, raw data files of one day (from find_daily_raw_datafile)
        analyzer: str, standard analyzer name (a key of id_dict below)
    Return:
        None (prints a warning if a wrong file is found)
    """

    # standard analyzer name -> start of its serial number
    id_dict = {'CO_Picarro': 'CFKADS', 'NH3_Picarro': 'AHDS', 'HCHO_Picarro': 'LBDS'}
    analyzer_id = id_dict[analyzer]

    for file in filepath:
        if analyzer_id not in file:
            date = filepath[-1].split('/')[-4:-1]  # ['YYYY', 'MM', 'DD'] from the folder path
            date = ''.join(date)
            print('Wrong ', analyzer, ' data files on ', date, '!')

            break  # only report the warning once


def daily_raw_data(filepath):
    """
    Read all raw data files of one day and combine them into one table.

    Input:
        filepath: list of str, raw data files of one day (from find_daily_raw_datafile)
    Return:
        df_daily: DataFrame, raw data of that day only, indexed by DATETIME
    """

    # the day being processed is read from the name of the last file, e.g. '...-20250101-230500Z-...'
    date = pd.to_datetime(filepath[-1].split('/')[-1].split('-')[1], format='%Y%m%d')

    df_daily = pd.DataFrame()

    for i in range(len(filepath)):

        # columns in the raw file are separated by spaces
        df = pd.read_csv(filepath[i], sep=r'\s+')

        # some files name the columns with '_sync' (e.g. 'CO_sync'); remove it so all files match
        df.columns = [col.replace('_sync', '') for col in df.columns]

        df_daily = pd.concat([df_daily, df])

    # keep only the rows of the day being processed
    df_daily = df_daily[pd.to_datetime(df_daily['DATE']) == date]

    df_daily['DATETIME'] = pd.to_datetime(df_daily['DATE'] + ' ' + df_daily['TIME'])
    df_daily = df_daily.set_index('DATETIME')

    return df_daily


def normal_inst_status(analyzer):
    """
    Return the INST_STATUS value that means "instrument OK" for the analyzer.
    (NH3 analyzer: 899, CO and HCHO analyzers: 963)
    """

    if analyzer == 'NH3_Picarro':
        return 899
    else:
        return 963


# Debugging step 2: check raw data alarm status (alarms reported by the analyzer)
def screen_warning_df_daily(df_daily, analyzer):
    """
    Print a warning if the analyzer reported any problem during the day.

    Checked columns:
        ALARM_STATUS: should always be 0 (alarm code)
        INST_STATUS:  should always be 963 (899 for the NH3 analyzer) (instrument status)
        MPVPosition:  should always be 0 (valve position)

    Input:
        df_daily: DataFrame, raw data of one day (from daily_raw_data)
        analyzer: str, standard analyzer name
    Return:
        None (prints warnings)
    """

    inst_status = normal_inst_status(analyzer)

    if df_daily['ALARM_STATUS'].sum() != 0:
        print('ALARM_STATUS is not all 0!')

    if df_daily['INST_STATUS'].sum() != inst_status * len(df_daily):
        print('INST_STATUS has warning!')

    if df_daily['MPVPosition'].sum() != 0:
        print('MPVPosition is not all 0!')


def clean_warning_data(df_daily, analyzer):
    """
    Remove the rows that have a warning (see screen_warning_df_daily).

    Input:
        df_daily: DataFrame, raw data of one day
        analyzer: str, standard analyzer name
    Return:
        df_daily_nowarning: DataFrame, raw data without the warning rows
    """

    inst_status = normal_inst_status(analyzer)

    df_daily_nowarning = df_daily.copy()
    df_daily_nowarning = df_daily_nowarning[
        (df_daily_nowarning['ALARM_STATUS'] == 0) &
        (df_daily_nowarning['INST_STATUS'] == inst_status) &
        (df_daily_nowarning['MPVPosition'] == 0)
    ]

    return df_daily_nowarning


def clean_parameter_column(df_daily_nowarning, analyzer):
    """
    Keep only the useful columns (date, time and measured concentrations).

    Input:
        df_daily_nowarning: DataFrame, raw data without warning rows
        analyzer: str, standard analyzer name
    Return:
        df_daily_clean: DataFrame, data with only the useful columns
    """

    # columns to keep for each analyzer
    column_dict = {'CO_Picarro': ['DATE', 'TIME', 'CO', 'CO2', 'CO2_dry',
                                  'CH4', 'CH4_dry', 'H2O'],
                   'HCHO_Picarro': ['DATE', 'TIME', 'H2CO', 'H2O', 'CH4'],
                   'NH3_Picarro': ['DATE', 'TIME', 'NH3', 'NH3_dry', 'H2O', 'CO2']}

    parameter_columns = column_dict[analyzer]

    df_daily_clean = df_daily_nowarning.copy()
    df_daily_clean = df_daily_clean[parameter_columns]

    return df_daily_clean


def average_daily_data(df_daily_clean, average_time, std=True, se=True):
    """
    Average the data to the chosen time resolution.

    For every measured column X, three columns are created:
        X      : mean
        X_std  : standard deviation
        X_se   : standard error (std / sqrt(number of points))

    Input:
        df_daily_clean: DataFrame, cleaned data of one day (indexed by DATETIME)
        average_time: str, averaging time (e.g. '1min', '60min')
        std, se: not used (std and se are always calculated); kept for compatibility
    Return:
        df_avg: DataFrame, averaged data (indexed by DATETIME)
    """

    df = df_daily_clean.copy()
    df.drop(columns=['DATE', 'TIME'], inplace=True)

    def calc_se(x):
        return x.std() / np.sqrt(len(x))

    # calculate mean, std and se of every column
    agg_funcs = {col: ['mean', 'std', calc_se] for col in df.columns}

    df_avg = df.resample(average_time).agg(agg_funcs)
    df_avg = df_avg.round(3)

    # rename the columns, e.g. ('CO', 'mean') -> 'CO', ('CO', 'std') -> 'CO_std'
    new_columns = []
    for col in df.columns:
        new_columns.extend([col, f"{col}_std", f"{col}_se"])
    df_avg.columns = new_columns

    return df_avg


def fill_missing_data(df_avg, average_time):
    """
    Add empty rows for missing times so every daily file has the full set of
    time steps (00:00 to 23:59 at the averaging resolution).

    Input:
        df_avg: DataFrame, averaged data (from average_daily_data)
        average_time: str, averaging time (e.g. '1min', '60min')
    Return:
        df_avg: DataFrame, averaged data with DATE and TIME columns and no missing time steps
    """

    df_avg['TIME'] = df_avg.index.time

    # table with every time step of the day
    time_range = pd.date_range(start='00:00', end='23:59', freq=average_time).time
    df_dt = pd.DataFrame(
        {'DATE': df_avg.index[0].date().strftime('%Y-%m-%d'),
         'TIME': time_range}
    )
    df_avg = pd.merge(df_dt, df_avg, on='TIME', how='left')

    return df_avg


def processed_data_folder_level1a(folder):
    """
    Return (and create if needed) the folder for 1-minute data.

    Input:
        folder: str, analyzer folder (from find_data_folder)
    Return:
        folder_level1a: str, '<folder>/Level1A_Processed_Data_1min'
    """

    folder_level1a = folder + '/Level1A_Processed_Data_1min'
    if not os.path.exists(folder_level1a):
        os.makedirs(folder_level1a)

    return folder_level1a


def processed_data_folder_level1b(folder):
    """
    Return (and create if needed) the folder for 1-hour data.

    Input:
        folder: str, analyzer folder (from find_data_folder)
    Return:
        folder_level1b: str, '<folder>/Level1B_Processed_Data_1hr'
    """

    folder_level1b = folder + '/Level1B_Processed_Data_1hr'
    if not os.path.exists(folder_level1b):
        os.makedirs(folder_level1b)

    return folder_level1b


def store_processed_data(folder_level1a, site, species, date, df_avg):
    """
    Save the processed data of one day as a CSV file.

    File: <folder_level1a>/YYYY/<Site>_Picarro-<species>_YYYYMMDD.csv
    DATE and TIME columns are renamed to DATE_UTC and TIME_UTC.
    Missing values are written as -9999.

    Input:
        folder_level1a: str, output folder (Level1A or Level1B folder)
        site: str, site name (e.g. 'Fresno-Garland Supersite')
        species: str, standard species name (from standardize_name)
        date: str or datetime, day processed
        df_avg: DataFrame, processed data of that day
    Return:
        None
    """

    # file name, e.g. 'Fresno_Picarro-CO2-CO-CH4_20250101.csv'
    site_name = site.split('-')[0]
    monitor_name = 'Picarro-' + species
    YY = pd.to_datetime(date).strftime('%Y')
    date = pd.to_datetime(date).strftime('%Y%m%d')

    filename = site_name + '_' + monitor_name + '_' + date + '.csv'
    filepath = folder_level1a + '/' + YY + '/' + filename

    # create the year folder if it does not exist
    if not os.path.exists(folder_level1a + '/' + YY):
        os.makedirs(folder_level1a + '/' + YY)

    df_avg.rename(columns={'DATE': 'DATE_UTC', 'TIME': 'TIME_UTC'}, inplace=True)

    df_avg.fillna(-9999, inplace=True)

    df_avg.to_csv(filepath, index=False)
    print('Data is stored: ', filename)


def main(site, analyzer, date, average_time='1min'):
    """
    Process the Picarro raw data of ONE day and save the result.

    Input:
        site: str, site name (e.g. 'Fresno-Garland Supersite')
        analyzer: str, analyzer name (e.g. 'NH3', 'CO', 'HCHO')
        date: str or datetime, day to process (e.g. '2025-01-31')
        average_time: str, '1min' or '60min'
    Return:
        None
    """

    if average_time not in VALID_AVERAGE_TIMES:
        raise ValueError(
            f"Invalid average time '{average_time}'. Valid options are: {VALID_AVERAGE_TIMES}"
        )

    analyzer, species = standardize_name(analyzer)

    folder = find_data_folder(site, analyzer)

    filepath = find_daily_raw_datafile(folder, date)

    if filepath is None:
        return  # no data on this day (message already printed)

    check_data_filename(filepath, analyzer)

    df_daily = daily_raw_data(filepath)

    screen_warning_df_daily(df_daily, analyzer)

    df_daily_nowarning = clean_warning_data(df_daily, analyzer)

    if df_daily_nowarning.empty:
        print('All data has warning on ', date, '!')
        return

    df_daily_clean = clean_parameter_column(df_daily_nowarning, analyzer)

    df_avg = average_daily_data(df_daily_clean, average_time, std=True, se=True)
    df_avg = fill_missing_data(df_avg, average_time)

    if average_time == '1min':
        output_folder = processed_data_folder_level1a(folder)
    else:  # '60min'
        output_folder = processed_data_folder_level1b(folder)

    store_processed_data(output_folder, site, species, date, df_avg)


if __name__ == "__main__":
    # process every day from START_DATE to END_DATE (settings at the top of this file)
    dates = pd.date_range(start=START_DATE, end=END_DATE, freq='D')

    for date in dates:
        main(SITE, ANALYZER, date, average_time=AVERAGE_TIME)

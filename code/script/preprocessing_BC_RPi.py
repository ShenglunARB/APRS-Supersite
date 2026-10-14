"""
Preprocess AE33 Black Carbon (BC) data that was logged by the Raspberry Pi (RPi).

Use this script for the RPi data files (Level0_Raw_Data_RPi/YYYY/data_log_YYYYMMDD...).
    - Normal AE33 data files saved by the instrument -> use preprocessing_BC.py

The RPi file only has date, time, BC1-BC7 and flow (no status / raw signal columns),
so only the flow is checked. Output files get "_RPi" in the folder and file names.

What this script does, for every day between START_DATE and END_DATE:
    1. Finds the RPi data file of that day (lines without data are skipped).
    2. Prints a warning if the flow is out of range.
    3. Removes bad rows (flow out of 5 L/min +/- 5%)
       and sets very negative BC values (< -100) to missing.
    4. Keeps the BC1-BC7 columns and fills missing minutes.
    5. Saves one CSV file per day:
         1min  -> Level1A_Processed_Data_1min_RPi/YYYY/<Site>_AE33-BC_YYYYMMDD_RPi.csv
         60min -> Level1B_Processed_Data_1hr_RPi/YYYY/<Site>_AE33-BC_YYYYMMDD_RPi.csv (mean, std, se)
       Missing values are written as -9999. All times are UTC.

How to use:
    1. Edit the "USER SETTINGS" block below.
    2. Save this file.
    3. Run it (see README.md), e.g. in the VS Code terminal:
         python preprocessing_BC_RPi.py

@author: Shenglun Wu
@date: Jan 2025
"""

import os
from io import StringIO

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

# Analyzer to process. Only option: 'AE33'
ANALYZER = 'AE33'

# First and last day to process (UTC), format 'YYYY-MM-DD'. Both days are included.
# To process only one day, use the same date for both.
START_DATE = '2025-04-14'
END_DATE = '2025-11-30'

# Averaging time. Options:
#   '1min'  -> saved in folder Level1A_Processed_Data_1min_RPi
#   '60min' -> saved in folder Level1B_Processed_Data_1hr_RPi
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
        analyzer: str, analyzer or species name ('AE33' or 'BC')
    Return:
        analyzer: str, standard analyzer (folder) name: 'BC_AE33'
        species: str, standard species name used in output file names: 'BC'
    """

    if analyzer in ['BC', 'AE33', 'BC_AE33']:
        analyzer = 'BC_AE33'
        species = 'BC'

    else:
        raise ValueError(f"Analyzer '{analyzer}' not found! Valid option is: 'AE33'")

    return analyzer, species


def find_data_folder(site, analyzer):
    """
    Return the folder that stores the data of one analyzer at one site.

    Input:
        site: str, site name (one of VALID_SITES)
        analyzer: str, standard analyzer name ('BC_AE33')
    Return:
        folder: str, e.g. '<BASE_PATH>/MWO/BC_AE33'
    """

    if site not in VALID_SITES:
        raise ValueError(f"Invalid site name '{site}'. Valid options are: {VALID_SITES}")

    valid_analyzers = ['BC_AE33']

    if analyzer not in valid_analyzers:
        raise ValueError(f"Invalid analyzer name '{analyzer}'. Valid options are: {valid_analyzers}")

    folder = f"{BASE_PATH}/{site}/{analyzer}"

    return folder


def find_daily_raw_datafile(folder, date):
    """
    Find the RPi data file of one day (one file per day).

    Input:
        folder: str, analyzer folder (from find_data_folder)
        date: str or datetime, day to process
    Return:
        filepath: str, path of the data file, or None if the year folder
                  is missing or zero / several files are found (a message is printed)
    """

    date = pd.to_datetime(date)
    YY = date.strftime('%Y')
    mm = date.strftime('%m')
    dd = date.strftime('%d')

    # RPi files are stored in Level0_Raw_Data_RPi/YYYY/
    folderpath = folder + '/Level0_Raw_Data_RPi/' + YY

    if not os.path.exists(folderpath):
        print('Folder not found on ', YY + mm + dd, '!')
        return None

    # look for a file named like 'data_log_YYYYMMDD...'
    filelist = os.listdir(folderpath)
    file_date = [file for file in filelist if 'data_log_' in file and YY + mm + dd in file]

    if file_date == []:
        print('No data file on ', YY + mm + dd, '!')
        return None

    elif len(file_date) > 1:
        print('Multiple data files on ', YY + mm + dd, '!')
        return None

    filepath = folderpath + '/' + file_date[0]

    return filepath


def daily_raw_data(filepath, date):
    """
    Read the RPi data file into a table.

    File format: comma separated, no header. Columns 2-11 are
    DATE (e.g. '15-Mar-25'), TIME, BC1-BC7 and FLOW (L/min).
    Lines without data (containing ',"",') are skipped.

    Input:
        filepath: str, data file (from find_daily_raw_datafile)
        date: str or datetime, day to process
    Return:
        df_daily: DataFrame, data of that day only, indexed by DATETIME
    """

    # lines with no data collected look like ',"","",'
    skip_pattern = ',"",'

    filtered_lines = []
    with open(filepath, 'r') as file:
        for line in file:
            if skip_pattern not in line:
                filtered_lines.append(line)

    # StringIO lets pandas read the filtered lines as if they were a file
    filtered_data = StringIO(''.join(filtered_lines))

    df = pd.read_csv(filtered_data, header=None)
    df_daily = df.iloc[:, 1:11].copy()
    df_daily.columns = ['DATE', 'TIME', 'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6', 'BC7', 'FLOW']

    # keep only the rows of the day being processed
    df_daily['DATE'] = pd.to_datetime(df_daily['DATE'], format='%d-%b-%y')
    df_daily = df_daily[df_daily['DATE'] == date]
    df_daily['DATE'] = df_daily['DATE'].dt.strftime('%Y-%m-%d')

    df_daily['TIME'] = df_daily['TIME'].astype(str)
    df_daily['DATETIME'] = pd.to_datetime(df_daily['DATE'] + ' ' + df_daily['TIME'])
    df_daily = df_daily.set_index('DATETIME')

    return df_daily


def screen_warning_df_daily(df_daily):
    """
    Print a warning if the flow was out of range during the day.

    Checked column:
        FLOW: should be 5 L/min +/- 5%

    Input:
        df_daily: DataFrame, data of one day (from daily_raw_data)
    Return:
        None (prints warnings)
    """

    # flow set point (L/min) and allowed tolerance (5%)
    flowc = 5
    tolerance = 0.05

    if not df_daily['FLOW'].between(flowc * (1 - tolerance), flowc * (1 + tolerance)).all():
        print('FlowC is out of the range 5 +/- 5%!')


def clean_warning_data(df_daily):
    """
    Remove bad data.

    Rows are removed when:
        - FLOW is outside 5 L/min +/- 5%
    Values are set to missing (NaN) when:
        - BC1-BC7 < -100

    Input:
        df_daily: DataFrame, data of one day
    Return:
        df_daily_nowarning: DataFrame, data without bad rows
    """

    # flow set point (L/min) and allowed tolerance (5%)
    flowc = 5
    tolerance = 0.05

    df_daily_nowarning = df_daily.copy()
    df_daily_nowarning = df_daily_nowarning[
        (df_daily_nowarning['FLOW'].between(flowc * (1 - tolerance), flowc * (1 + tolerance)))
    ]

    parameter = ['BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6', 'BC7']
    for p in parameter:
        df_daily_nowarning.loc[df_daily_nowarning[p] < -100, p] = np.nan

    return df_daily_nowarning


def clean_parameter_column(df_daily_nowarning):
    """
    Keep only the useful columns (date, time and BC1-BC7).

    Input:
        df_daily_nowarning: DataFrame, data without bad rows
    Return:
        df_daily_clean: DataFrame, data with only the useful columns
    """

    parameter_columns = ['DATE', 'TIME',
                         'BC1', 'BC2', 'BC3', 'BC4', 'BC5', 'BC6', 'BC7']

    df_daily_clean = df_daily_nowarning.copy()
    df_daily_clean = df_daily_clean[parameter_columns]

    return df_daily_clean


def fill_missing_data(df):
    """
    Add empty rows for missing minutes so every daily file has all
    1440 minutes (00:00 to 23:59).

    Input:
        df: DataFrame, cleaned data of one day (indexed by DATETIME)
    Return:
        df: DataFrame, data with no missing minutes (indexed by DATETIME)
    """

    df['TIME'] = df.index.time
    df.drop(columns=['DATE'], inplace=True)

    # table with every minute of the day
    time_range = pd.date_range(start='00:00', end='23:59', freq='1min').time
    df_dt = pd.DataFrame(
        {'DATE': df.index[0].date().strftime('%Y-%m-%d'),
         'TIME': time_range}
    )
    df = pd.merge(df_dt, df, on=['TIME'], how='left')

    df['DATETIME'] = pd.to_datetime(df['DATE'].astype(str) + ' ' + df['TIME'].astype(str))
    df.set_index('DATETIME', inplace=True)

    return df


def average_daily_data(df_daily_clean, average_time, std=True, se=True):
    """
    Average the data to the chosen time resolution.

    For every column X, three columns are created:
        X      : mean
        X_std  : standard deviation
        X_se   : standard error (std / sqrt(number of points))

    Input:
        df_daily_clean: DataFrame, cleaned data of one day (indexed by DATETIME)
        average_time: str, averaging time (e.g. '60min')
        std, se: not used (std and se are always calculated); kept for compatibility
    Return:
        df_avg: DataFrame, averaged data with DATE and TIME columns
    """

    df = df_daily_clean.copy()
    df.drop(columns=['DATE', 'TIME'], inplace=True)

    def calc_se(x):
        return x.std() / np.sqrt(len(x))

    # calculate mean, std and se of every column
    agg_funcs = {col: ['mean', 'std', calc_se] for col in df.columns}

    df_avg = df.resample(average_time).agg(agg_funcs)
    df_avg = df_avg.round(3)

    # rename the columns, e.g. ('BC1', 'mean') -> 'BC1', ('BC1', 'std') -> 'BC1_std'
    new_columns = []
    for col in df.columns:
        new_columns.extend([col, f"{col}_std", f"{col}_se"])
    df_avg.columns = new_columns

    df_avg['DATE'] = df_avg.index.date
    df_avg['TIME'] = df_avg.index.time
    df_avg = df_avg[['DATE', 'TIME'] + new_columns]

    return df_avg


def processed_data_folder_level1a(folder):
    """
    Return (and create if needed) the folder for 1-minute RPi data.

    Input:
        folder: str, analyzer folder (from find_data_folder)
    Return:
        folder_level1a: str, '<folder>/Level1A_Processed_Data_1min_RPi'
    """

    folder_level1a = folder + '/Level1A_Processed_Data_1min_RPi'
    if not os.path.exists(folder_level1a):
        os.makedirs(folder_level1a)

    return folder_level1a


def processed_data_folder_level1b(folder):
    """
    Return (and create if needed) the folder for 1-hour RPi data.

    Input:
        folder: str, analyzer folder (from find_data_folder)
    Return:
        folder_level1b: str, '<folder>/Level1B_Processed_Data_1hr_RPi'
    """

    folder_level1b = folder + '/Level1B_Processed_Data_1hr_RPi'
    if not os.path.exists(folder_level1b):
        os.makedirs(folder_level1b)

    return folder_level1b


def store_processed_data(folder_level1a, site, species, date, df_avg):
    """
    Save the processed data of one day as a CSV file.

    File: <folder_level1a>/YYYY/<Site>_AE33-<species>_YYYYMMDD_RPi.csv
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

    # file name, e.g. 'MWO_AE33-BC_20250101_RPi.csv'
    site_name = site.split('-')[0]
    monitor_name = 'AE33-' + species
    YY = pd.to_datetime(date).strftime('%Y')
    date = pd.to_datetime(date).strftime('%Y%m%d')

    filename = site_name + '_' + monitor_name + '_' + date + '_RPi.csv'
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
    Process the RPi AE33 data of ONE day and save the result.

    Input:
        site: str, site name (e.g. 'Fresno-Garland Supersite')
        analyzer: str, analyzer name ('AE33')
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

    df_daily = daily_raw_data(filepath, date)
    screen_warning_df_daily(df_daily)
    df_daily_nowarning = clean_warning_data(df_daily)

    if df_daily_nowarning.empty:
        print('All data has warning on ', date, '!')
        return

    df_daily_clean = clean_parameter_column(df_daily_nowarning)
    df_daily_clean = fill_missing_data(df_daily_clean)

    if average_time == '1min':
        folder_level1a = processed_data_folder_level1a(folder)
        store_processed_data(folder_level1a, site, species, date, df_daily_clean)

    else:  # '60min'
        df_avg = average_daily_data(df_daily_clean, average_time)
        folder_level1b = processed_data_folder_level1b(folder)
        store_processed_data(folder_level1b, site, species, date, df_avg)


if __name__ == "__main__":
    # process every day from START_DATE to END_DATE (settings at the top of this file)
    dates = pd.date_range(start=START_DATE, end=END_DATE, freq='D')

    for date in dates:
        main(SITE, ANALYZER, date, average_time=AVERAGE_TIME)

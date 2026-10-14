# CARB RD APRS Supersite Data Processing

Author: Shenglun Wu, shenglun.wu@arb.ca.gov

This repository processes the raw data from the supersites run by the California Air
Resources Board (CARB) Research Division (RD) Atmospheric Processing Research Section (APRS).

- **Sites:** Fresno (Fresno-Garland Supersite), Bakersfield (Bakersfield-California Ave Supersite), MWO
- **Instruments:**
  - Picarro analyzers: CO (with CO2, CH4), HCHO, NH3
  - AE33 Black Carbon (BC) analyzer
  - miniMPL (boundary layer): no script yet

Each script reads the raw data (Level 0), removes data with warnings, averages it to
1-minute or 1-hour resolution, and saves one CSV file per day (Level 1A / Level 1B).

---

## Contents

1. [What's in this repository](#1-whats-in-this-repository)
2. [Which script should I use?](#2-which-script-should-i-use)
3. [First-time setup (VS Code)](#3-first-time-setup-vs-code)
4. [How to run a script](#4-how-to-run-a-script)
5. [Where the data is (input and output)](#5-where-the-data-is-input-and-output)
6. [What the messages mean](#6-what-the-messages-mean)
7. [Troubleshooting](#7-troubleshooting)

---

## 1. What's in this repository

```
APRS-Supersite/
├── code/
│   └── script/
│       ├── preprocessing_Picarro.py                 <- Picarro CO, HCHO, NH3
│       ├── preprocessing_BC.py                      <- AE33 black carbon (normal)
│       ├── preprocessing_BC_RPi.py                  <- AE33 data logged by the Raspberry Pi
│       └── preprocessing_BC_NeedTimeZoneChange.py   <- AE33 when its clock was not set to UTC
├── output/              <- put your run logs (.out files) here
├── requirements.txt     <- list of Python packages to install
└── README.md            <- this file
```

Each script works the same way: **change the settings at the top, save, and run.**

---

## 2. Which script should I use?

| I want to process...                                    | Use this script                           |
|---------------------------------------------------------|-------------------------------------------|
| Picarro CO / CO2 / CH4 (G2401)                          | `preprocessing_Picarro.py`, `ANALYZER = 'CO'`   |
| Picarro HCHO (G2307)                                    | `preprocessing_Picarro.py`, `ANALYZER = 'HCHO'` |
| Picarro NH3 (G2103)                                     | `preprocessing_Picarro.py`, `ANALYZER = 'NH3'`  |
| AE33 black carbon, files saved by the AE33 (`Level0_Raw_Data`) | `preprocessing_BC.py`              |
| AE33 black carbon, files logged by the Raspberry Pi (`Level0_Raw_Data_RPi`) | `preprocessing_BC_RPi.py` |
| AE33 black carbon during a period when the AE33 clock was wrong (see below) | `preprocessing_BC_NeedTimeZoneChange.py` |

Periods when the AE33 clock was **not** UTC (the times are in instrument clock time):

| Site        | Period                                  | Correction       |
|-------------|-----------------------------------------|------------------|
| Fresno      | before 2024-05-02 21:39                 | +7 h             |
| Bakersfield | 2025-02-03 00:00 to 2025-10-14 08:32    | +7 h 58 min      |
| MWO         | before 2024-10-16 20:04                 | +7 h             |

`preprocessing_BC_NeedTimeZoneChange.py` only changes the times in these periods, so it is
safe to use around the start and end dates of a period. If you find a new clock problem, add it to the
`timezone_correction` function in that script.

---

## 3. First-time setup (VS Code)

You only need to do this once per computer.

### 3.1 Install the software

1. **Python 3.11 or 3.12:** download it from <https://www.python.org/downloads/>.
   In the installer, **check "Add python.exe to PATH"**.
2. **VS Code:** download it from <https://code.visualstudio.com/>.
3. **Git:** download it from <https://git-scm.com/download/win>. You need it to download this repository.
4. **OneDrive:** make sure the APRS SharePoint folder `!Site Operations` is synced to your computer.
   In SharePoint, open *Shared Documents - RD ASCSB APRS*, then click **Add shortcut to My files**
   (or **Sync**).

### 3.2 Get the code

In VS Code: press `Ctrl+Shift+P`, type **Git: Clone**, paste
`https://github.com/ShenglunARB/APRS-Supersite.git`, choose a folder, then click **Open**.

### 3.3 Install the Python extension

When VS Code opens the folder, it asks you to install the recommended **Python** extension. Click **Install**.
(Or open the Extensions panel with `Ctrl+Shift+X`, search **Python** (by Microsoft), and click Install.)

### 3.4 Create a virtual environment and install packages

A *virtual environment* (`.venv`) is a private copy of Python for this project, with its own packages.

1. Press `Ctrl+Shift+P` and run **Python: Create Environment**.
2. Choose **Venv**, then choose your Python 3.11/3.12.
3. When it asks about dependencies, **check `requirements.txt`** and click OK.
4. Wait until a `.venv` folder appears in the Explorer panel.

To check that it worked, open a terminal (**Terminal → New Terminal**). The prompt should start with
`(.venv)`. Then type:

```powershell
python -c "import pandas; print('pandas', pandas.__version__)"
```

If you see a version number, you are ready.

<details>
<summary>Prefer typing commands? Same steps in the terminal</summary>

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```
</details>

### 3.5 Check the data folder path (`BASE_PATH`)

At the top of every script there is a `BASE_PATH` setting that points to the `!Site Operations` folder:

```python
BASE_PATH = os.path.expanduser("~") + "/OneDrive - California Air Resources Board/Shared Documents - RD ASCSB APRS/General/02. In-House Research/!Site Operations"
```

`os.path.expanduser("~")` means your home folder (`C:/Users/<your user name>`), so if you synced the SharePoint
folder as in step 3.1, **you don't need to change anything**. If your folder is somewhere else, find it in File
Explorer, copy the path from the address bar, and use it with forward slashes `/`:

```python
BASE_PATH = "C:/Users/abc/OneDrive - California Air Resources Board/Shared Documents - RD ASCSB APRS/General/02. In-House Research/!Site Operations"
```

---

## 4. How to run a script

### Step 1: Change the settings

Open the script (for example `code/script/preprocessing_BC.py`). At the top there is a **USER SETTINGS** block.
This is the only part you need to change:

```python
SITE = 'Bakersfield-California Ave Supersite'  # 'Fresno-Garland Supersite', 'Bakersfield-California Ave Supersite', 'MWO'
ANALYZER = 'AE33'                              # Picarro: 'CO', 'HCHO', 'NH3'   |   BC: 'AE33'
START_DATE = '2025-01-01'                      # first day (UTC), 'YYYY-MM-DD'
END_DATE = '2025-01-31'                        # last day (UTC), included
AVERAGE_TIME = '1min'                          # '1min' or '60min'
```

The example above processes Bakersfield AE33 data for January 2025 at 1-minute resolution.
To process a single day, set `START_DATE` and `END_DATE` to the same date.

### Step 2: Save the file (`Ctrl+S`)

### Step 3: Run it

**Option A, quick run:** click the **▶ Run Python File** button at the top right of the editor.
Messages appear in the terminal at the bottom.

**Option B, run and save a log (recommended for long runs):** in the VS Code terminal, go to the script folder
and run the script. All messages are shown on screen **and** saved to a `.out` file in the `output` folder:

```powershell
cd code\script
python -u preprocessing_BC.py | Tee-Object -FilePath ..\..\output\preprocessing_Bakersfield_AE33_20250131.out
```

Suggested log file name: `preprocessing_{Site}_{Species}_{DateOfProcessing}.out`.
(`-u` makes Python print each message immediately instead of in batches.)

<details>
<summary>Using Git Bash instead (runs in the background)</summary>

```bash
cd code/script
nohup python preprocessing_Picarro.py > ../../output/preprocessing_Fresno_Picarro-CO_20241227.out &
```
</details>

### Step 4: Check the messages

Read the terminal or the `.out` file. Every successful day prints `Data is stored: <file name>`.
For any other message, see [section 6](#6-what-the-messages-mean).

> **Note:** Running a script again for the same days **overwrites** the existing output files.

---

## 5. Where the data is (input and output)

All data is in the `!Site Operations` folder (`BASE_PATH`), organized like this:

```
!Site Operations/
└── <Site>/                                   e.g. Fresno-Garland Supersite, Bakersfield-California Ave Supersite, MWO
    ├── CO_Picarro/   (also HCHO_Picarro/, NH3_Picarro/)
    │   ├── Level0_Raw_Data/YYYY/MM/DD/       INPUT:  hourly raw files, e.g. CFKADS2454-20250101-004222Z-DataLog_User.dat
    │   ├── Level1A_Processed_Data_1min/YYYY/ OUTPUT: 1-minute data
    │   └── Level1B_Processed_Data_1hr/YYYY/  OUTPUT: 1-hour data
    └── BC_AE33/
        ├── Level0_Raw_Data/YYYY/             INPUT:  daily AE33 files, e.g. AE33_AE33-S08-01023_20250315.dat
        ├── Level0_Raw_Data_RPi/YYYY/         INPUT:  daily Raspberry Pi files, e.g. data_log_20250515.txt
        ├── Level1A_Processed_Data_1min/YYYY/ OUTPUT: 1-minute data   (RPi: ..._1min_RPi)
        └── Level1B_Processed_Data_1hr/YYYY/  OUTPUT: 1-hour data     (RPi: ..._1hr_RPi)
```

**Output files:** one CSV per day, for example:

| Script         | Example output file name                   |
|----------------|--------------------------------------------|
| Picarro CO     | `Fresno_Picarro-CO2-CO-CH4_20250101.csv`   |
| Picarro HCHO   | `MWO_Picarro-HCHO_20250101.csv`            |
| Picarro NH3    | `Bakersfield_Picarro-NH3-H2O_20250101.csv` |
| AE33           | `Fresno_AE33-BC_20250101.csv`              |
| AE33 (RPi)     | `MWO_AE33-BC_20250101_RPi.csv`             |

**Output columns:**

- `DATE_UTC`, `TIME_UTC`: date and time in **UTC**. The time is the **start** of the averaging period.
- One column per measured value (e.g. `CO`, `CH4`, `BC1` … `BC7`).
- `X_std` and `X_se`: standard deviation and standard error of value `X` in each averaging period.
  (Not included in AE33 1-minute files, which are the raw 1-minute values.)
- **`-9999` means missing data.** Every file has every time step of the day (1440 rows for 1 min, 24 rows for 1 h),
  even when there was no data.

**Data cleaning rules:**

| Instrument | Rows removed when                                            | Values set to missing |
|------------|--------------------------------------------------------------|-----------------------|
| Picarro    | `ALARM_STATUS` ≠ 0, `INST_STATUS` ≠ 963 (NH3: 899), `MPVPosition` ≠ 0 | — |
| AE33       | `Status` ≠ 0, `FlowC` outside 5 L/min ± 5 %, `RefCh1` = 0    | BC1–BC7 < -100        |
| AE33 (RPi) | `FLOW` outside 5 L/min ± 5 %                                 | BC1–BC7 < -100        |

---

## 6. What the messages mean

Bad rows are always removed **automatically**. A warning only tells you that something happened that day, so you
can check the instrument logbook if needed.

| Message | Meaning | What to do |
|---------|---------|------------|
| `Data is stored: <file>` | The day was processed and saved. | Nothing. |
| `Folder not found on YYYYMMDD !` | There is no raw data folder for that day (or year). | Normal if the instrument was off. Otherwise check that the data was uploaded and that OneDrive finished syncing. |
| `No data file on YYYYMMDD !` | The folder exists but has no data file for that day. | Same as above. |
| `Multiple data files on YYYYMMDD !` | (AE33) More than one data file for that day. **The day is skipped.** | Look in the raw folder and remove the duplicate file, then run that day again. |
| `No data on <date> !` | (Time-zone script) No data for the day after the time correction. | Same as "Folder not found". |
| `Wrong CO_Picarro data files on YYYYMMDD !` | (Picarro) A file in the folder comes from a different analyzer (the serial number in the file name doesn't match). | Check whether files were copied into the wrong folder. |
| `ALARM_STATUS is not all 0!` | (Picarro) The analyzer reported an alarm. | Check the instrument log. |
| `INST_STATUS has warning!` | (Picarro) The instrument was not in normal status (e.g., warming up). | Check the instrument log. |
| `MPVPosition is not all 0!` | (Picarro) The valve was not in sampling position (e.g., calibration). | Usually a calibration. Check the logbook. |
| `Status is not all 0!` | (AE33) The AE33 reported a status code (e.g., tape advance, warning). | Check the AE33 log. |
| `FlowC is out of the range 5 +/- 5%!` | (AE33) The flow was not 5 L/min ± 5 %. | Check the flow and the pump. |
| `All data has warning on <date> !` | Every row of the day was removed. **No file is saved.** | Check the instrument for that day. |

---

## 7. Troubleshooting

| Problem | Solution |
|---------|----------|
| `ModuleNotFoundError: No module named 'pandas'` | The virtual environment is not active. Click the Python version in the bottom-right corner of VS Code and choose the one with `.venv`, then open a **new** terminal. Or run `pip install -r requirements.txt` again. |
| `.\.venv\Scripts\Activate.ps1 cannot be loaded because running scripts is disabled` | Run `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` once in the terminal, then try again. |
| `python` is not recognized | Python is not on your PATH. Reinstall Python and check **"Add python.exe to PATH"**, then restart VS Code. |
| `ValueError: Invalid site name ...` / `Analyzer ... not found` / `Invalid average time ...` | There is a typo in the USER SETTINGS. The error message lists the valid options. Copy one exactly, including capital letters and spaces. |
| Every day says `Folder not found` | `BASE_PATH` is wrong or OneDrive is not synced. Open the path in File Explorer to check. |
| The script is slow or freezes on the first file | OneDrive may be downloading files that were "online-only". Right-click the site folder in File Explorer and choose **Always keep on this device**. |
| `PermissionError` when saving | The output CSV is open in Excel. Close it and run again. |
| Stop a running script | Click in the terminal and press `Ctrl+C`. |

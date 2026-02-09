# CARB RD APRS Supersite Data Processing


author: Shenglun Wu, shenglun.wu@arb.ca.gov


- __Description__: this repository used to process the raw data from supersites operated by California Air Resources Board (CARB) Research Division (RD) Atmospheric Processing Research Section (APRS). 

- __Supersite locations__: Fresno, Bakersfield, MWO

- __Instruments__: 
  - Picarro analyzers: CO, HCHO, NH3
  - AE33 Black Carbon (BC) analyzer
  - miniMPL: measureing boundary layer

- __Usage__:
  - **preprocessing**: retrive raw data, clean warning data, averaging data to 1min / 1hr resolution. 
    - **Scripts**: preprocessing_Picarro.py, preprocessing_BC.py
    - **Before use script**: Change the "base_path" (folder path for "!Site Operation" in Section's SharePoint Folder) under function "find_data_folder" in all .py scripts. Example: 
        ```python
        base_path = "C:/Users/swu/OneDrive - California Air Resources Board/Shared Documents - RD ASCSB APRS/General/In-House Research/!Site Operations" 
        ```

    - **Use script:** 
      1. Specify the "site", "analyzer", and "date" under ```if __name__ == "__main__"``` in the script. Below is an example of a monthly (Jan 2025) data preprocessing of AE33 (BC) in Bakersfield:
          ```python
          site = 'Bakersfield-California Ave Supersite'
          analyzer = 'AE33'
          dates = pd.date_range(start='2025-01-01', end='2025-01-31', freq='D')
          ```

      2. Save the script
      
      3. In Terminal goes to the ./code/src, then run the script. If use Git Bash as Terminal, use the code:
          ```python
          # run the script and save the output to a .out file (a text file)
          nohup python preprocessing_Picarro.py > ../../output/preprossing_{Location}_{Species}_{DateofProcessing}.out &
    
          # example
          nohup python preprocessing_Picarro.py > ../../output/preprocessing_Fresno_Picarro-CO_20241227.out &
          ```
          
      4. Check .out file to see any warning need to be checked.

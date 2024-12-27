# CARB RD APRS Supersite Data Processing


author: Shenglun Wu, shenglun.wu@arb.ca.gov


- __Description__: this repository used to process the raw data from supersites operated by California Air Resources Board (CARB) Research Division (RD) Atmospheric Processing Research Section (APRS). 

- __Supersite locations__: Fresno, Bakersfield, MWO

- __Instruments__: 
  - Picarro analyzers: CO, HCHO, NH3
  - AE33 Black Carbon (BC) analyzer
  - miniMPL: measureing boundary layer

- __Usage__:
  - preprocessing: retrive raw data, clean warning data, averaging data to 1min / 1hr resolution. 
    - Use script: preprocessing_Picarro.py, preprocessing_BC.py
    - Change the "site", "analyzer", and "date" in main function in the script.
    - Run the script in Git Bash  
        ```python
        nohup preprocessing_Picarro.py > ../../output/preprossing_{Location}_{Species}_{DateofProcessing}.out &
    
        # example
        nohup preprocessing_Picarro.py > ../../output/preprocessing_Fresno_Picarro-CO.out &
        ```
    - Check .out file to see any warning need to be checked.




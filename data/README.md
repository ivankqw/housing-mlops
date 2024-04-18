# Data Directory

### General Housing and Locality Data
- `districts.xlsx`: This dataset contains information on the districts in Singapore
- `districts_transformed.csv`: This is a preprocessed dataset containing information on the districts in Singapore
- `hdb_information.csv`: This dataset contains information on Housing & Development Board (HDB) flats in Singapore
- `addresses.csv`: This dataset contains information on the addresses of HDB flats in Singapore (latitude and longitude coordinates)

### Resale Flats Data
- `resale_flat_transactions.csv`: This dataset contains detailed records of resale transactions for Housing & Development Board (HDB) flats
- `resale_flats_transformed.csv`: This is a preprocessed dataset containing detailed records of resale transactions for Housing & Development Board (HDB) flats

### Economic Indicators
- `cpi_with_lag_sma_ema.csv`: This dataset comprises the Consumer Price Index (CPI) with additional computed features like lagged values, simple moving average (SMA), and exponential moving average (EMA). These features are designed to help in the analysis and prediction of economic trends.
- `sibor_sora.csv`: It contains data on the Singapore Interbank Offered Rate (SIBOR) and the Singapore Overnight Rate Average (SORA). These are key interest rates that are used in financial feature engineering, particularly relevant for projects dealing with economic forecasting or financial services.

## Miscellaneous Data
**The data that are not used in the scope of this project but are useful in following sprints out-of-scope**
- `private_transactions.csv`: This dataset contains records of private property transactions in Singapore. The data includes features such as project name, street name, property type, district, tenure, price, and date of sale.
- `private_transactions_transformed.csv`: This is a preprocessed dataset containing records of private property transactions in Singapore. The data includes features such as project name, street name, property type, district, tenure, price, and date of sale.

## Usage
For more comprehensive details on the generation, preprocessing, and specific use cases of these datasets, please refer to the README documentation in the `mlops` and `dataops` directories.

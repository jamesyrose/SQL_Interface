#### SQL Interface Scripts
Script allows access to MySQL through Python. 

Specifically used for OHLCV Stock Market Data. 

Data is split by ticker symbol and year to minimize query times. SQL's Union command is used to join data when queried. 

All prices are stored as Integers (multiplied by 10**4) to maintain 1/100 second price resolution

Use this script to query database, SQL commands can get long. 

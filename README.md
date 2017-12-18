# zillow
Methods in these scripts clean ZTRAX data provided by Zillow, then intersect the data with UCS chronic inundation layers to determine properties exposed to chronic inundation today or in the future. 

Cleaning scripts are in the .idea folder. The primary script to use is zillow_exploration.

The cleaning process involves importing ZTRAX data into a sql database, performing joins, and removing duplicate properties or those without proper addresses. After cleaning, data are exported to csvs from the command line. 

The csvs are then imported into ArcGIS databases for spatial analysis. Spatial analysis methods are in the  intersect_zillow_ci_layers.py file in the main directory of this repo. Geocoding during spatial analysis steps was performed on csv files by Geocod.io--another relatively manual step.

Note that a separate level of cleaning was done for FL because I was running into data size constraints.

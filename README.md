# Metabase Starburst Driver

## Driver Development

Make sure to clone curefit/cf-dataplatform-metabase. Make sure to follow the local set up of Metabase in README.md of that repo.
You need to set MB_LOCAL_METABASE_DIR and MB_LOCAL_STARBURST_DIR environment variables to build and test the driver.

eg:
```
MB_LOCAL_METABASE_DIR=/Users/john.doe/cf-dataplatform-metabase
MB_LOCAL_STARBURST_DIR=/Users/john.doe/starburst
```

Run `make build` in the root directory of this repo to build the driver.
This step automatically builds the driver into the Metabase repo.
Post this step you can rerun the Metabase server locally to test the driver changes.  

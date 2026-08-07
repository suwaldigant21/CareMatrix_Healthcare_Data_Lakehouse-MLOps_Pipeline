# CareMatrix Lakehouse - docs/

## Power BI report
`docs/CareMatrix_Analytics.pbix` - Power BI Desktop report connected to the
Athena Gold layer via the Simba Athena ODBC driver.

## Screenshots
`docs/screenshots/` contains captured evidence of the end-to-end build, including:
- Power BI dashboard views (`power bi dashboard.png`)
- Athena query results (`athena query results 1.png`, `athena query results showing
  claim year, total claims and total payment amount.png`)
- AWS Glue PySpark job run + success (`Pyspark code run on aws glue.png`, `Pyspark job succeded.png`)
- AWS Crawler output (`aws crawlers doing their thing.png`)
- dbt lineage + docs (`dbt flow.png`, `dbt docs 1.png`, `dbt docs 2.png`)
- Data cleaning evidence (`dataset should be cleaned because of this.png`, `after cleaning found this.png`)
- Power BI live data loading + ODBC connectivity (`loading live data on powerbi.png`, `Powe bi connected using ODBC.png`)
- Reference methodology tooling (`sas studio working.png`, `how i used sas studio.png`)

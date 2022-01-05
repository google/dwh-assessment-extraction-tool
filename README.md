# Data Warehouse Assessment Extraction Client

This tool allows to extract meta information from a data warehouse that allows
to make an assessment for migration.

## License

Copyright 2021 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Extraction tool user guide
The extraction tool is currently intended for approved users that are engaging 
with GCP technical sales teams.

**Step 1:** Follow the user guide from the GCP technical sales team and download 
the Extraction tool binary (i.e., ExtractionTool_deploy.jar) and its run script.

**Step 2:** Download [Teradata JDBC driver](https://downloads.teradata.com/download/connectivity/jdbc-driver) into the same directory with the Extraction tool binary.

**Step 3:** Create a folder for output files and run the Extraction tool:
```build
./run-td-extract.sh -j <terajdbc4.jar> --db-address <database address> --output <output path> --db-user <db user>
```

**Optional parameters:**

`--schema-filter`  The schema filter to apply when extracting schemas from the database. By default, all schemas will be extracted.
- Example usage: `--schema-filter db:(abc|def),table:public_.`
  Only take schemas from tables in the database abc or from tables whose names have the prefix public_.
- Multiple filters can be defined by repeating the option. Each filter has to match (i.e. AND logic).

`--sql-scripts`  The list of scripts to execute. By default, all available scripts will be executed.
- Available scripts extract metadata from tables:
    - [All_RI_ChildrenV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/CDg3b4d71cfITbRjAAQM5Q)
    - [All_RI_ParentsV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/KjM9MSv3K5G5Q_gCTNtGZw)
    - [ColumnsV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/fQ8NslP6DDESV0ZiODLlIw)
    - [DiskSpaceV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/ZhJCNhtQ1i4llpxKkYn7eA)
    - [FunctionsV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/hx9hvPb9EvS6TP9Ta2PUzQ)
    - [IndicesV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/qkWdqMUH7HZaIkY_pSQUng)
    - [QryLog](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/sT8ifzajeQ9jMx7ciiu1dA)
    - [TablesV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/JKGDTOsfv6_gr8wswcE9eA)
    - [TableSizeV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/qQd_5O6fT0QrDcSfDEZj~Q)
    - [UsersV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/rES2eYXMN2IBoFBIPIWz0Q)
- [Click to view the scripts.](https://team.git.corp.google.com/edwmigration-eng/dwh-assessment-extraction-tool/+/refs/heads/master/src/java/com/google/cloud/bigquery/dwhassessment/extractiontool/dbscripts/)

`--skip-sql-scripts` The list of scripts to skip. By default, all available scripts will be executed.

**Step 4:** The extraction process may take from a few minutes to hours to finish,
depending on the amount of data in your database. Share the output files with
the PSO Cloud Consultant.
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

## Building 

## Extraction tool user guide
The extraction tool is currently intended for approved users that are engaging
with GCP technical sales teams.

**Step 1:** Follow the user guide from the GCP technical sales team and download
the Extraction tool binary (i.e., ExtractionTool_deploy.jar) and its run script.

**Step 2:** Download [Teradata JDBC driver](https://downloads.teradata.com/download/connectivity/jdbc-driver) into the same directory with the Extraction tool binary.

**Step 3:** Create a folder for output files and run the Extraction tool:

Option 1: basic run:
```bash
./run-td-extract.sh -j <terajdbc4.jar> --db-address <database address> --output <output path> --db-user <db user>
```
Option 2: compatible with the full set of optional parameters (See **Optional parameters** section):
```bash
./dwh-assessment-execution-tool.sh --db-address <database address> --output <output path> --db-user <db user>
```

**Optional Parameters:**

`--schema-filter`  The schema filter to apply when extracting schemas from the database. By default, all schemas will be extracted.
- Example usage: `--schema-filter db:(abc|def),table:public_.`
  Only take schemas from tables in the database abc or from tables whose names have the prefix public_.
- Multiple filters can be defined by repeating the option. Each filter has to match (i.e. AND logic).

`--sql-scripts`  The list of scripts to execute. By default, all available scripts will be executed.
- Available scripts extract metadata from tables and views:
  - [AllTempTablesVX](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/FVKCCZtalFF_UOT2PcPfrA)
  - [All_RI_ChildrenV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/CDg3b4d71cfITbRjAAQM5Q)
  - [All_RI_ParentsV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/KjM9MSv3K5G5Q_gCTNtGZw)
  - [ColumnsV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/fQ8NslP6DDESV0ZiODLlIw)
  - [DatabasesV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/GqTx8VuBIkfaC4fso9f5cw)
  - [DiskSpaceV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/ZhJCNhtQ1i4llpxKkYn7eA)
  - [FunctionsV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/hx9hvPb9EvS6TP9Ta2PUzQ)
  - [IndicesV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/qkWdqMUH7HZaIkY_pSQUng)
  - [PartitioningConstraintsV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/G5eOtdk_Z5xjAgAderQlQg)
  - [DBQLObjTbl](https://docs.teradata.com/r/B7Lgdw6r3719WUyiCSJcgw/eOMXq~u5PwRV5GrooD6_9A)
  - [QryLogV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/sT8ifzajeQ9jMx7ciiu1dA)
  - [QryLogSQLV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/SQqDiRkDlOLYNSZ4dBRIGQ)
  - [RoleMembersV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/y5EHNeWIu1uFk5714KHTaw)
  - [StatsV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/nLsDT6mdwnn1QrOG35ttMw)
  - [TablesV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/JKGDTOsfv6_gr8wswcE9eA)
  - [TableSizeV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/qQd_5O6fT0QrDcSfDEZj~Q)
  - [TableTextV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/h0HJktc4jYAtjKjVExdjUw)
  - [UsersV](https://docs.teradata.com/r/oiS9ixs2ypIQvjTUOJfgoA/rES2eYXMN2IBoFBIPIWz0Q)
- [Click to view the scripts.](src/java/com/google/cloud/bigquery/dwhassessment/extractiontool/dbscripts)

`--skip-sql-scripts` The list of scripts to skip. By default, all available scripts will be executed.

For additional options, please run
```bash
./dwh-assessment-extraction-tool.sh td-extract -h
```

**Step 4:** The extraction process may take from a few minutes to hours to finish,
depending on the amount of data in your database. Share the output files with
the PSO Cloud Consultant.
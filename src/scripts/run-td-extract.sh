#!/bin/bash
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This script is a helper script for running assessment extraction client
# for extracting data from Teradata.
#
# Usage: ./run -j <path to terajdbc4.jar> [-e -p <extracted_path>]
# Options:
#   -j: teradata jdbc jar.

while [[ $# -gt 0 ]]
do
key="$1"
case $key in
    -j|--terajdbc4_jar)
    TERAJDBC4_JAR="$2"
    shift # past argument
    shift # past value
    ;;
    --db-address)
    DB_ADDRESS="$2"
    shift # past argument
    shift # past value
    ;;
    --output)
    OUTPUT="$2"
    shift # past argument
    shift # past value
    ;;
    --db-user)
    DB_USER="$2"
    shift # past argument
    shift # past value
    ;;
    --db-password)
    DB_PASSWORD="$2"
    shift # past argument
    shift # past value
    ;;
    --schema-filter)
    SCHEMA_FILTER="$2"
    shift # past argument
    shift # past value
    ;;
    --sql-scripts)
    SQL_SCRIPTS="$2"
    shift # past argument
    shift # past value
    ;;
    --skip-sql-scripts)
    SKIP_SQL_SCRIPTS="$2"
    shift # past argument
    shift # past value
    ;;
esac
done

echo "TERAJDBC4 JAR = ""$TERAJDBC4_JAR"""
echo "DB ADDRESS = ""$DB_ADDRESS"""
echo "OUTPUT PATH = ""$OUTPUT"""

if [[ -z "${TERAJDBC4_JAR}" || -z "${DB_ADDRESS}" || -z "${OUTPUT}" || -z "${DB_USER}" ]]; then
    echo "Missing required arguments. Usage: ${0} -j <terajdbc4.jar> --db-address <database address> --output <output path> --db-user <db user>"
    exit 1
fi

args=( --db-address "${DB_ADDRESS}" --output "${OUTPUT}" --db-user "${DB_USER}")

if [[ -n "${DB_PASSWORD}" ]]; then
    args+=( --db-password "${DB_PASSWORD}" )
else
  args+=( --db-password )
fi

if [[ -n "${SCHEMA_FILTER}" ]]; then
    args+=( --schema-filter "${SCHEMA_FILTER}" )
fi

if [[ -n "${SQL_SCRIPTS}" ]]; then
    args+=( --sql-scripts "${SQL_SCRIPTS}" )
fi

if [[ -n "${SKIP_SQL_SCRIPTS}" ]]; then
    args+=( --skip-sql-scripts "${SKIP_SQL_SCRIPTS}" )
fi

CLASSPATH="$(dirname "$0")/ExtractionTool_deploy.jar:${TERAJDBC4_JAR}"
java -cp "${CLASSPATH}" \
  com/google/cloud/bigquery/dwhassessment/extractiontool/ExtractionTool \
  td-extract "${args[@]}"

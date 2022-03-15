/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.integration;

import static com.google.avro.AvroUtil.extractAvroDataFromFile;
import static com.google.avro.AvroUtil.getIntNotNull;
import static com.google.avro.AvroUtil.getStringNotNull;
import static com.google.tdjdbc.JdbcUtil.getIntNotNull;
import static com.google.tdjdbc.JdbcUtil.getStringNotNull;
import static java.lang.String.format;

import com.google.base.TestBase;
import com.google.common.collect.LinkedHashMultiset;
import com.google.pojo.SchemaRow;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaTest extends TestBase {

  private static Connection connection;
  private static final String AVRO_FILE_PATH = ET_OUTPUT_PATH + "schema.avro";

  private static final Logger LOGGER = LoggerFactory.getLogger(TestBase.class);

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void schemaTest() throws SQLException, IOException {
    LinkedHashMultiset<SchemaRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<SchemaRow> avroList = LinkedHashMultiset.create();
    Set<String> tableNameSet = new HashSet<>();

    try (ResultSet tablesResultSet =
        connection
            .getMetaData()
            .getTables(
                /*catalog =*/ null,
                /*schemaPattern =*/ null,
                /*tableNamePattern =*/ null,
                /*columnNamePattern =*/ new String[] {"TABLE"})) {
      while (tablesResultSet.next()) {
        tableNameSet.add(tablesResultSet.getString("TABLE_NAME"));
      }
    }

    for (String tableName : tableNameSet) {
      try (ResultSet rs =
          connection
              .getMetaData()
              .getColumns(
                  /*catalog =*/ null,
                  /*schemaPattern =*/ null,
                  /*tableNamePattern =*/ tableName,
                  /*columnNamePattern =*/ null)) {
        while (rs.next()) {
          SchemaRow dbRow =
              SchemaRow.create(
                  getStringNotNull(rs, "TABLE_CAT"),
                  getStringNotNull(rs, "TABLE_SCHEM"),
                  getStringNotNull(rs, "TABLE_NAME"),
                  getStringNotNull(rs, "COLUMN_NAME"),
                  getIntNotNull(rs, "DATA_TYPE"),
                  getStringNotNull(rs, "TYPE_NAME"),
                  getIntNotNull(rs, "COLUMN_SIZE"),
                  getIntNotNull(rs, "BUFFER_LENGTH"),
                  getIntNotNull(rs, "DECIMAL_DIGITS"),
                  getIntNotNull(rs, "NUM_PREC_RADIX"),
                  getIntNotNull(rs, "NULLABLE"),
                  getStringNotNull(rs, "REMARKS"),
                  getStringNotNull(rs, "COLUMN_DEF"),
                  getStringNotNull(rs, "SQL_DATA_TYPE"),
                  getStringNotNull(rs, "SQL_DATETIME_SUB"),
                  getIntNotNull(rs, "CHAR_OCTET_LENGTH"),
                  getIntNotNull(rs, "ORDINAL_POSITION"),
                  getStringNotNull(rs, "IS_NULLABLE"),
                  getStringNotNull(rs, "SCOPE_CATLOG"),
                  getStringNotNull(rs, "SCOPE_SCHEMA"),
                  getStringNotNull(rs, "SCOPE_TABLE"),
                  getIntNotNull(rs, "SOURCE_DATA_TYPE"));
          dbList.add(dbRow);
        }
      }
    }

    DataFileReader<GenericRecord> dataFileReader = extractAvroDataFromFile(AVRO_FILE_PATH);

    while (dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();

      SchemaRow avroRow =
          SchemaRow.create(
              getStringNotNull(record, "TABLE_CAT"),
              getStringNotNull(record, "TABLE_SCHEM"),
              getStringNotNull(record, "TABLE_NAME"),
              getStringNotNull(record, "COLUMN_NAME"),
              getIntNotNull(record, "DATA_TYPE"),
              getStringNotNull(record, "TYPE_NAME"),
              getIntNotNull(record, "COLUMN_SIZE"),
              getIntNotNull(record, "BUFFER_LENGTH"),
              getIntNotNull(record, "DECIMAL_DIGITS"),
              getIntNotNull(record, "NUM_PREC_RADIX"),
              getIntNotNull(record, "NULLABLE"),
              getStringNotNull(record, "REMARKS"),
              getStringNotNull(record, "COLUMN_DEF"),
              getStringNotNull(record, "SQL_DATA_TYPE"),
              getStringNotNull(record, "SQL_DATETIME_SUB"),
              getIntNotNull(record, "CHAR_OCTET_LENGTH"),
              getIntNotNull(record, "ORDINAL_POSITION"),
              getStringNotNull(record, "IS_NULLABLE"),
              getStringNotNull(record, "SCOPE_CATLOG"),
              getStringNotNull(record, "SCOPE_SCHEMA"),
              getStringNotNull(record, "SCOPE_TABLE"),
              getIntNotNull(record, "SOURCE_DATA_TYPE"));
      avroList.add(avroRow);
    }

    LOGGER.info(format("Total number of unique DB tables: %s", tableNameSet.size()));
    LOGGER.info(format("Total number of DB columns: %s", dbList.size()));
    LOGGER.info(format("Total number of Avro columns: %s", avroList.size()));

    LinkedHashMultiset<SchemaRow> dbListCopy = LinkedHashMultiset.create(dbList);
    avroList.forEach(dbList::remove);
    dbListCopy.forEach(avroList::remove);

    assertListsEqual(dbList, avroList, "DB Schema", AVRO_FILE_PATH);
  }
}

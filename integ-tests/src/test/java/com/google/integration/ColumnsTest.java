package com.google.integration;

import static com.google.avro.AvroHelper.extractAvroDataFromFile;
import static com.google.avro.AvroHelper.getIntNotNull;
import static com.google.avro.AvroHelper.getStringNotNull;
import static com.google.tdjdbc.JdbcHelper.getIntNotNull;
import static com.google.tdjdbc.JdbcHelper.getStringNotNull;
import static java.lang.String.format;

import com.google.base.TestBase;
import com.google.common.collect.LinkedHashMultiset;
import com.google.pojo.ColumnRow;
import com.google.sql.SqlHelper;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.junit.BeforeClass;
import org.junit.Test;

public class ColumnsTest extends TestBase {

  private static Connection connection;

  @BeforeClass
  public static void beforeClass() throws SQLException {
    connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);
  }

  @Test
  public void columnsTest() throws SQLException, IOException {
    final String sqlPath = "src/main/java/com/google/sql/columns.sql";
    final String avroFilePath = ET_OUTPUT_PATH + "columns.avro";

    LinkedHashMultiset<ColumnRow> dbList = LinkedHashMultiset.create();
    LinkedHashMultiset<ColumnRow> avroList = LinkedHashMultiset.create();

    try (PreparedStatement preparedStatement =
        connection.prepareStatement(format(SqlHelper.getSql(sqlPath), DB_NAME, DB_NAME))) {
      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        ColumnRow dbRow = ColumnRow.create(
            getStringNotNull(rs, "DataBaseName"),
            getStringNotNull(rs, "TableName"),
            getStringNotNull(rs, "ColumnName"),
            getStringNotNull(rs, "ColumnFormat"),
            getStringNotNull(rs, "ColumnTitle"),
            getIntNotNull(rs, "ColumnLength"),
            getStringNotNull(rs, "ColumnType"),
            getStringNotNull(rs, "DefaultValue"),
            getStringNotNull(rs, "ColumnConstraint"),
            getIntNotNull(rs, "ConstraintCount"),
            getStringNotNull(rs, "Nullable"));
        dbList.add(dbRow);
      }
    }

    DataFileReader<GenericRecord> dataFileReader = extractAvroDataFromFile(avroFilePath);

    while (dataFileReader.hasNext()) {
      GenericRecord record = null;
      record = dataFileReader.next(record);

      ColumnRow avroRow = ColumnRow.create(
          getStringNotNull(record, "DataBaseName"),
          getStringNotNull(record, "TableName"),
          getStringNotNull(record, "ColumnName"),
          getStringNotNull(record, "ColumnFormat"),
          getStringNotNull(record, "ColumnTitle"),
          getIntNotNull(record, "ColumnLength"),
          getStringNotNull(record, "ColumnType"),
          getStringNotNull(record, "DefaultValue"),
          getStringNotNull(record, "ColumnConstraint"),
          getIntNotNull(record, "ConstraintCount"),
          getStringNotNull(record, "Nullable"));
      avroList.add(avroRow);
    }

    LinkedHashMultiset<ColumnRow> dbListCopy = LinkedHashMultiset.create(dbList);
    avroList.forEach(dbList::remove);
    dbListCopy.forEach(avroList::remove);

    assertListsEqual(dbList, avroList, sqlPath, avroFilePath);
  }
}

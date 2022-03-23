package com.google.performance;

import static com.google.base.TestConstants.PASSWORD_DB;
import static com.google.base.TestConstants.SQL_TESTDATA_BASE_PATH;
import static com.google.base.TestConstants.URL_DB;
import static com.google.base.TestConstants.USERNAME_DB;
import static com.google.sql.SqlUtil.executeQueries;
import static com.google.sql.SqlUtil.getSql;
import static com.google.testdata.TestDataHelper.getRandomDbName;
import static java.lang.String.format;

import com.google.testdata.FunctionalTestDataHelper;
import com.google.testdata.pojo.TestDataUser;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class IndicesPerformanceTest {

  @DataProvider(parallel = true)
  public Object[][] usersDp() throws IOException, CsvException {
    FileReader filereader = new FileReader("target/userList.csv");
    CSVReader csvReader = new CSVReaderBuilder(filereader).build();
    List<String[]> userList = csvReader.readAll();

    String[][] userArray = new String[userList.size()][3];
    for (int i = 0; i < userList.size(); i++) {
      userArray[i] = userList.get(i);
    }

    return userArray;
  }

  @DataProvider(parallel = true, indices = {10,20,30,40,50})
  public Object[][] usersDpWithStaticTable() throws IOException, CsvException {
    FileReader filereader = new FileReader("target/userList.csv");
    CSVReader csvReader = new CSVReaderBuilder(filereader).build();
    List<String[]> userList = csvReader.readAll();

    String[][] userArray = new String[userList.size()][2];
    for (int i = 0; i < userList.size(); i++) {
      userArray[i] = userList.get(i);
    }

    return userArray;
  }

  @Test(dataProvider = "usersDp")
  public void indicesPerformanceTest(String username, String password, String dbName) throws SQLException {
    FunctionalTestDataHelper.generateTables(username, password, dbName, 100);
  }

  @Test(dataProvider = "usersDpWithStaticTable")
  public void indicesPerformanceTestWithStaticTable(String username, String password) throws SQLException {
    String dbName = "PerfTestDb";
    FunctionalTestDataHelper.generateTablesForDb(username, password, dbName, 10000);
  }

  @Test
  public void usersGeneration() throws SQLException, IOException {
    final String createUserSql = getSql(SQL_TESTDATA_BASE_PATH + "users_data.sql");
    final String createDbSql = getSql(SQL_TESTDATA_BASE_PATH + "performance_data_1.sql");
    final String grantCreateTableSql = getSql(SQL_TESTDATA_BASE_PATH + "performance_data_2.sql");

    Connection connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);

    List<String[]> userList = new ArrayList<>();
    int userCount = 100;
    for (int i = 0; i < userCount; i++) {
      int SIZE = 2048 * 10000;
      String dbName = getRandomDbName();
      TestDataUser userData = new TestDataUser();

      String createUser = format(createUserSql, userData.getUsername(), userData.getPassword());
      String createDatabase = format(createDbSql, dbName, SIZE, SIZE, SIZE);
      String grantCreateTable = format(grantCreateTableSql, dbName, userData.getUsername());

      executeQueries(connection, Arrays.asList(createUser, createDatabase, grantCreateTable));
      userList.add(new String[] {userData.getUsername(), userData.getPassword(), dbName});
    }

    File file = new File("target/userList.csv");
    FileWriter outputfile = new FileWriter(file);

    CSVWriter writer = new CSVWriter(outputfile);
    writer.writeAll(userList);
    writer.close();
  }

  @Test
  public void usersGenerationWithStaticTable() throws SQLException, IOException {
    final String createUserSql = getSql(SQL_TESTDATA_BASE_PATH + "users_data.sql");
    final String grantCreateTableSql = getSql(SQL_TESTDATA_BASE_PATH + "performance_data_2.sql");
    final String dbName = "PerfTestDb";

    Connection connection = DriverManager.getConnection(URL_DB, USERNAME_DB, PASSWORD_DB);

    List<String[]> userList = new ArrayList<>();
    int userCount = 100;
    for (int i = 0; i < userCount; i++) {
      TestDataUser userData = new TestDataUser();

      String createUser = format(createUserSql, userData.getUsername(), userData.getPassword());
      String grantCreateTable = format(grantCreateTableSql, dbName, userData.getUsername());

      executeQueries(connection, Arrays.asList(createUser, grantCreateTable));
      userList.add(new String[] {userData.getUsername(), userData.getPassword()});
    }

    File file = new File("target/userList.csv");
    FileWriter outputfile = new FileWriter(file);

    CSVWriter writer = new CSVWriter(outputfile);
    writer.writeAll(userList);
    writer.close();
  }
}

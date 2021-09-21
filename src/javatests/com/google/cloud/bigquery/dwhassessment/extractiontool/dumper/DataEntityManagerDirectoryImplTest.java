package com.google.cloud.bigquery.dwhassessment.extractiontool.dumper;

import static com.google.common.io.Files.asCharSource;
import static com.google.common.truth.Truth.assertThat;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataEntityManagerDirectoryImplTest {

  private Path tmpDir;

  @Before
  public void setUp() throws IOException {
    tmpDir = Files.createTempDirectory("data-entity-manager-directory-test");
  }

  @Test
  public void testDataEntityManager() throws IOException {
    DataEntityManager dataEntityManager = new DataEntityManagerDirectoryImpl(tmpDir);
    try (OutputStream out = dataEntityManager.getEntityOutputStream("test1")) {
      out.write("test1".getBytes(StandardCharsets.UTF_8));
    }
    try (OutputStream out = dataEntityManager.getEntityOutputStream("test2")) {
      out.write("Hello There".getBytes(StandardCharsets.UTF_8));
    }

    assertThat(
            Files.walk(tmpDir)
                .filter(Files::isRegularFile)
                .sorted()
                .collect(
                    ImmutableMap.toImmutableMap(
                        path -> path.getFileName().toString(),
                        path -> {
                          try {
                            return asCharSource(path.toFile(), StandardCharsets.UTF_8).read();
                          } catch (IOException e) {
                            throw new RuntimeException(e);
                          }
                        })))
        .isEqualTo(ImmutableMap.of("test1", "test1", "test2", "Hello There"));
  }
}

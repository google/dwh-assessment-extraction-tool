package com.google.cloud.bigquery.dwhassessment.extractiontool.dumper;

import static com.google.common.truth.Truth.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataEntityManagerFactoryTest extends TestCase {

  private final Function<Path, DataEntityManager> factory = new DataEntityManagerFactory();
  private Path tmpDir;

  @Before
  public void setUp() throws IOException {
    tmpDir = Files.createTempDirectory("data-entity-manager-factory-test");
  }

  @Test
  public void testApply_directory_success() {
    assertThat(factory.apply(tmpDir)).isInstanceOf(DataEntityManagerDirectoryImpl.class);
  }

  @Test
  public void testApply_zip_success() {
    assertThat(factory.apply(tmpDir.resolve("foo.zip")))
        .isInstanceOf(DataEntityManagerZipImpl.class);
  }
}

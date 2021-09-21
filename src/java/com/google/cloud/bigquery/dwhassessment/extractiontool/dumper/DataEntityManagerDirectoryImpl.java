package com.google.cloud.bigquery.dwhassessment.extractiontool.dumper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** A data entity manager that writes files to a given directory. */
public class DataEntityManagerDirectoryImpl implements DataEntityManager {

  private final Path basePath;

  public DataEntityManagerDirectoryImpl(Path basePath) {
    this.basePath = basePath;
  }

  @Override
  public OutputStream getEntityOutputStream(String name) throws IOException {
    return Files.newOutputStream(basePath.resolve(name));
  }

  @Override
  public void close() throws IOException {}
}

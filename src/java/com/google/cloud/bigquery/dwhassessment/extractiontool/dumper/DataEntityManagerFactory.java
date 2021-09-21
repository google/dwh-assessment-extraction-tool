package com.google.cloud.bigquery.dwhassessment.extractiontool.dumper;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;

public class DataEntityManagerFactory implements Function<Path, DataEntityManager> {

  @Override
  public DataEntityManager apply(Path path) {
    if (path.toString().endsWith(".zip")) {
      Preconditions.checkArgument(
          Files.isDirectory(path.getParent()), "%s is not a directory.", path.getParent());
      try {
        return new DataEntityManagerZipImpl(Files.newOutputStream(path));
      } catch (IOException e) {
        throw new IllegalStateException(
            String.format("Failed to initialize the DataEntityManager for path: %s", path));
      }
    }
    Preconditions.checkArgument(Files.isDirectory(path), "%s is not a directory.", path);
    return new DataEntityManagerDirectoryImpl(path);
  }
}

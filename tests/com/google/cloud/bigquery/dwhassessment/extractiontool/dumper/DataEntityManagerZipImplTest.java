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

package com.google.cloud.bigquery.dwhassessment.extractiontool.dumper;

import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipOutputStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;

@RunWith(JUnit4.class)
public class DataEntityManagerZipImplTest {

  private ZipOutputStream zipOutputStream;

  @Before
  public void setUp() {
    zipOutputStream = mock(ZipOutputStream.class);
  }

  @Test
  public void getEntityOutputStream_writeSingleEntityToZip() throws IOException {
    DataEntityManagerZipImpl manager = new DataEntityManagerZipImpl(zipOutputStream);

    OutputStream fooOutputStream = manager.getEntityOutputStream("foo");
    fooOutputStream.write(3);
    fooOutputStream.close();

    InOrder inOrder = inOrder(zipOutputStream);
    inOrder.verify(zipOutputStream)
        .putNextEntry(argThat(zipEntry -> zipEntry.getName().equals("foo")));
    inOrder.verify(zipOutputStream).write(3);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void getEntityOutputStream_writeMultipleEntitiesToZip() throws IOException {
    DataEntityManagerZipImpl manager = new DataEntityManagerZipImpl(zipOutputStream);

    OutputStream fooOutputStream = manager.getEntityOutputStream("foo");
    fooOutputStream.write(1);
    fooOutputStream.close();

    OutputStream barOutputStream = manager.getEntityOutputStream("bar");
    barOutputStream.write(2);
    barOutputStream.close();

    InOrder inOrder = inOrder(zipOutputStream);
    inOrder.verify(zipOutputStream)
        .putNextEntry(argThat(zipEntry -> zipEntry.getName().equals("foo")));
    inOrder.verify(zipOutputStream).write(1);
    inOrder.verify(zipOutputStream)
        .putNextEntry(argThat(zipEntry -> zipEntry.getName().equals("bar")));
    inOrder.verify(zipOutputStream).write(2);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void getEntityOutputStream_failOnConcurrentAccess() {
    DataEntityManagerZipImpl manager = new DataEntityManagerZipImpl(zipOutputStream);
    manager.getEntityOutputStream("foo");

    assertThrows(IllegalStateException.class, () -> manager.getEntityOutputStream("bar"));
  }

  @Test
  public void close_closeZipOutputStream() throws IOException {
    DataEntityManagerZipImpl manager = new DataEntityManagerZipImpl(zipOutputStream);
    manager.close();

    verify(zipOutputStream).close();
  }
}

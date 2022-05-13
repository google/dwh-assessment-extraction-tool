package com.google.cloud.bigquery.dwhassessment.extractiontool.subcommand;

import static com.google.common.truth.Truth.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import picocli.CommandLine;

@RunWith(JUnit4.class)
public class AboutSubcommandTest {

  @Test
  public void call_success() {
    CommandLine cmd = new CommandLine(new AboutSubcommand());

    assertThat(cmd.execute()).isEqualTo(0);
  }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import net.hydromatic.quidem.Quidem;

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;

import java.io.File;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Runs every Quidem file in the "core" module with top down optimizer as a test.
 */
class TopDownCoreQuidemTest extends CoreQuidemTest {

  /** Runs a test from the command line.
   *
   * <p>For example:
   *
   * <blockquote>
   *   <code>java TopDownCoreQuidemTest sql/dummy.iq</code>
   * </blockquote> */
  public static void main(String[] args) throws Exception {
    for (String arg : args) {
      new TopDownCoreQuidemTest().test(arg);
    }
  }

  /** For {@link QuidemTest#test(String)} parameters. */
  public static Collection<Object[]> data() {
    // Start with a test file we know exists, then find the directory and list
    // its files.
    final String first = "sql/agg.iq";
    return data(first).stream()
        // do not run sequence.iq as it it not stabled
        .filter(a-> !Objects.equals(a[0], "sql/sequence.iq"))
        .collect(Collectors.toList());
  }

  @Override protected String checkDiff(File inFile, File outFile) {
    String diff = super.checkDiff(inFile, outFile);
    if (diff.isEmpty()) {
      return diff;
    }
    String diffFileName = inFile.getAbsolutePath() + ".topdown";
    File diffFile = new File(diffFileName);
    if (!diffFile.exists()) {
      return diff;
    }

    List<String> expectedDiffLines = DiffTestCase.fileLines(diffFile).
        stream().filter(a -> !a.startsWith("#")).collect(Collectors.toList());
    List<String> actualDiffLines = Arrays.asList(diff.split("\n"));
    String diff4Diff = DiffTestCase.diffLines(expectedDiffLines, actualDiffLines);
    if (diff4Diff.isEmpty()) {
      return diff4Diff;
    }
    return diff;
  }

  @Override protected Quidem.ConnectionFactory createConnectionFactory() {
    return new TopDownQuidemConnectionFactory();
  }

  protected static class TopDownQuidemConnectionFactory extends QuidemConnectionFactory {
    @Override public Connection connect(String name, boolean reference) throws Exception {
      Connection connection = super.connect(name, reference);
      if (connection instanceof CalciteConnection) {
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        Properties properties = calciteConnection.getProperties();
        properties.setProperty(CalciteConnectionProperty.TOPDOWN_OPT.camelName(), "true");
      }
      return connection;
    }
  }
}

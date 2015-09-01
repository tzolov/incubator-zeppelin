/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.zeppelin.springxd;

import static org.apache.zeppelin.springxd.ResourceCompletion.LINE_SEPARATOR;
import static org.apache.zeppelin.springxd.ResourceCompletion.convertXdToZeppelinCompletions;
import static org.apache.zeppelin.springxd.ResourceCompletion.getCompletionPreffix;
import static org.apache.zeppelin.springxd.ResourceCompletion.EMPTY_ZEPPELIN_COMPLETION;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/**
 * Unit tests for CompletionUtil
 */
public class CompletionUtilTest {


  @Test
  public void testGetCompletionPreffixSingleLine() {
    String multilineBufffer = "012 456";

    assertEquals("", getCompletionPreffix(multilineBufffer, 0));
    assertEquals("01", getCompletionPreffix(multilineBufffer, 2));
    assertEquals("012 456", getCompletionPreffix(multilineBufffer, 7));
    assertEquals("012 456", getCompletionPreffix(multilineBufffer, 8));
    assertEquals("012 456", getCompletionPreffix(multilineBufffer, 100));
  }

  // @Test
  public void testGetCompletionPreffixMultiLine() {
    String line1 = "012 456";
    String line2 = "654 210";
    String line3 = "987 564";

    String multilineBufffer = line1 + LINE_SEPARATOR + line2 + LINE_SEPARATOR + line3;

    // line 1
    assertEquals("", getCompletionPreffix(multilineBufffer, 0));
    assertEquals("01", getCompletionPreffix(multilineBufffer, 2));
    assertEquals("012 456", getCompletionPreffix(multilineBufffer, line1.length()));

    // line 2
    assertEquals("", getCompletionPreffix(multilineBufffer, 8));
    assertEquals("65", getCompletionPreffix(multilineBufffer, 10));
    assertEquals("654 210",
        getCompletionPreffix(multilineBufffer, (line1 + LINE_SEPARATOR + line2).length()));

    // line 3
    assertEquals("", getCompletionPreffix(multilineBufffer, 16));
    assertEquals("98", getCompletionPreffix(multilineBufffer, 18));
    assertEquals("987 564", getCompletionPreffix(multilineBufffer, multilineBufffer.length()));
  }

  // @Test
  public void testConvertXdToZeppelinCompletions() {

    List<String> xdCompletions = Arrays.asList("boza one", "boza two", "boza three");

    assertEquals(Arrays.asList("boza one", "boza two", "boza three"),
        convertXdToZeppelinCompletions(xdCompletions, ""));
    assertEquals(Arrays.asList("boza one", "boza two", "boza three"),
        convertXdToZeppelinCompletions(xdCompletions, null));

    assertEquals(Arrays.asList("one", "two", "three"),
        convertXdToZeppelinCompletions(xdCompletions, "boza "));

    assertEquals(EMPTY_ZEPPELIN_COMPLETION, convertXdToZeppelinCompletions(null, "fsadfasdf"));
    assertEquals(EMPTY_ZEPPELIN_COMPLETION,
        convertXdToZeppelinCompletions(new ArrayList<String>(), "fsadfasdf"));
  }
}

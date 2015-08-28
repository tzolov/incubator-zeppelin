package org.apache.zeppelin.springxd;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author tzoloc
 *
 */
public class Main {

  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  /**
   * @param args
   */
  public static void main(String[] args) {
    String buf = "boza \n\n\n koza";
    System.out.println(getCursorLine(buf, 5));
  }

  private static Pair<String, Integer> getCursorLine(String buf, int cursor) {

    String[] lines = buf.split(LINE_SEPARATOR);

    int processedSize = 0;
    for (String line : lines) {
      if (cursor < (processedSize + line.length())) {
        return new ImmutablePair<String, Integer>(line, cursor - processedSize);
      }
      processedSize = processedSize + line.length();
    }

    return new ImmutablePair<String, Integer>("", 0);
  }
}

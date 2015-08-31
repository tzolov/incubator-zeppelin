package org.apache.zeppelin.springxd;

import static org.apache.commons.lang.StringUtils.isBlank;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Helper class.
 *
 */
public class CompletionUtils {

  public static final String LINE_SEPARATOR = System.getProperty("line.separator");

  public static String getSearchPreffix(String buf, int cursor) {
    Pair<String, Integer> cursorLine = CompletionUtils.getCursorLine(buf, cursor);

    String preffix = cursorLine.getLeft().substring(0, cursorLine.getRight() - 1);

    return preffix;
  }

  /**
   * @param multilineBufffer
   * @param cursor
   * @return For multiline buffer returns the line pointed by the input cursor parameter and the
   *         cursor position relative to that line
   */
  public static Pair<String, Integer> getCursorLine(String multilineBufffer, int cursor) {

    // logger.info("In Buffer:" + buf.replace(LINE_SEPARATOR, "_") + ":" + cursor);

    String[] lines = multilineBufffer.split(LINE_SEPARATOR);

    int processedSize = 0;
    for (String line : lines) {
      int lineLength = line.length() + LINE_SEPARATOR.length();
      // logger.info(line + ":" + lineLength + " : " + (processedSize + line.length()));
      if (cursor <= (processedSize + lineLength)) {
        return new ImmutablePair<String, Integer>(line, cursor - processedSize);
      }
      processedSize += lineLength;
    }

    return new ImmutablePair<String, Integer>("", 0);
  }

  public static List<String> convertXdToZeppelinCompletions(List<String> xdCompletions,
      String preffix) {

    List<String> zeppelinCompletions = null;

    if (!CollectionUtils.isEmpty(xdCompletions)) {
      zeppelinCompletions = new ArrayList<String>();
      String preffixToReplace =
          preffix.substring(0, CompletionUtils.getLastWhitespaceIndex(preffix) + 1);
      for (String c : xdCompletions) {

        String zeppelinCompletion = c.replace(preffixToReplace, "").trim();
        // logger.info(preffix + "[" + preffixToReplace + "] -> " + c + " -> " +
        // zeppelinCompletion);
        zeppelinCompletions.add(zeppelinCompletion);
      }
    }

    return zeppelinCompletions;
  }

  private static int getLastWhitespaceIndex(String s) {
    if (!isBlank(s)) {
      for (int i = s.length() - 1; i >= 0; i--) {
        if (Character.isWhitespace(s.charAt(i)) || s.charAt(i) == '|' || s.charAt(i) == '=') {
          return i;
        }
      }
    }
    return 0;
  }
}

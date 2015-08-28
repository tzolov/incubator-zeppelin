package org.apache.zeppelin.springxd;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.xd.rest.client.impl.SpringXDTemplate;
import org.springframework.xd.rest.domain.CompletionKind;
import org.springframework.xd.rest.domain.StreamDefinitionResource;

import com.google.common.base.Throwables;


/**
 * 
 *
 */
public class SpringXdStreamInterpreter extends Interpreter {

  private Logger logger = LoggerFactory.getLogger(SpringXdStreamInterpreter.class);

  private static final String LINE_SEPARATOR = System.getProperty("line.separator");
  private static final Pattern NAMED_STREAM_PATTERN = Pattern.compile("\\s*(\\w*)\\s*=\\s*(.*)");

  public static final String SPRINGXD_URL = "springxd.url";
  public static final String DEFAULT_SPRINGXD_URL = "http://localhost:9393";

  private List<StreamDefinitionResource> deployedStreams;
  private Exception exceptionOnConnect;

  static {
    Interpreter.register(
        "stream",
        "xd",
        SpringXdStreamInterpreter.class.getName(),
        new InterpreterPropertyBuilder().add(SPRINGXD_URL, DEFAULT_SPRINGXD_URL,
            "The URL for SpringXD REST API.").build());
  }

  private SpringXDTemplate xdTemplate;

  public SpringXdStreamInterpreter(Properties property) {
    super(property);
    deployedStreams = new ArrayList<StreamDefinitionResource>();
  }

  @Override
  public void open() {
    // Destroy any previously deployed streams
    close();
    try {
      String springXdUrl = getProperty(SPRINGXD_URL);
      xdTemplate = new SpringXDTemplate(new URI(springXdUrl));
      exceptionOnConnect = null;
    } catch (URISyntaxException e) {
      logger.error("Failed to connect to the SpringXD cluster", e);
      exceptionOnConnect = e;
      close();
    }
  }

  @Override
  public void close() {
    destroyDeployedStreams();
    // xdTemplate.jobOperations().destroyAll();
    // xdTemplate.streamOperations().destroyAll();
    xdTemplate = null;
  }

  private void destroyDeployedStreams() {
    for (StreamDefinitionResource stream : deployedStreams) {
      xdTemplate.streamOperations().destroy(stream.getName());
      logger.info("Stream destroyed: " + stream.getName());
    }
    deployedStreams.clear();
  }

  @Override
  public InterpreterResult interpret(String multiLineStreamDefinitions, InterpreterContext ctx) {

    if (exceptionOnConnect != null) {
      return new InterpreterResult(Code.ERROR, exceptionOnConnect.getMessage());
    }

    // (Re)deploying streams means that the previous instances will be destroyed
    destroyDeployedStreams();

    String errorMessage = "";
    try {
      int deployedStreamCount = 0;

      if (!StringUtils.isBlank(multiLineStreamDefinitions)) {
        for (String line : multiLineStreamDefinitions.split(LINE_SEPARATOR)) {
          Pair<String, String> namedStream = getNamedStream(line);
          String streamName = namedStream.getLeft();
          String streamDef = namedStream.getRight();
          if (!StringUtils.isBlank(streamName) && !StringUtils.isBlank(streamDef)) {
            StreamDefinitionResource stream =
                xdTemplate.streamOperations().createStream(streamName, streamDef, true);
            deployedStreams.add(stream);
            deployedStreamCount++;
            logger.info("Deployed Stream: [" + streamName + "]:[" + streamDef + "]");
          } else {
            logger.info("Skipped Line:" + line);
          }
        }
      }

      return new InterpreterResult(Code.SUCCESS, " Deployed:" + deployedStreamCount + " Streams");

    } catch (Exception e) {
      logger.error("Failed to deploy streams!", e);
      errorMessage = Throwables.getRootCause(e).getMessage();
      close();
    }

    return new InterpreterResult(Code.ERROR, "Failed to deploy Streams: " + errorMessage);
  }

  private Pair<String, String> getNamedStream(String line) {
    Matcher matcher = NAMED_STREAM_PATTERN.matcher(line);
    String streamName = "";
    String streamDefinition = "";

    if (matcher.matches()) {
      streamName = matcher.group(1);
      streamDefinition = matcher.group(2);
    }

    return new ImmutablePair<String, String>(streamName, streamDefinition);
  }


  @Override
  public void cancel(InterpreterContext context) {}

  @Override
  public FormType getFormType() {
    return FormType.NONE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    List<String> completions = null;
    if (!StringUtils.isBlank(buf)) {
      try {
        Pair<String, Integer> cursorLine = getCursorLine(buf, cursor);

        String preffix = cursorLine.getLeft().substring(0, cursorLine.getRight() - 1);
        logger.info("Completion Request:" + cursorLine.toString() + ":" + preffix);

        List<String> completions2 =
            xdTemplate.completionOperations().completions(CompletionKind.stream, preffix, 1);

        if (!CollectionUtils.isEmpty(completions2)) {
          completions = new ArrayList<String>();
          String preffixToReplace = preffix.substring(0, getLastWhitespaceIndex(preffix));
          for (String c : completions2) {

            logger.info(preffix + "[" + preffixToReplace + "] -> " + c + " -> "
                + c.replace(preffixToReplace, ""));
            completions.add(c.replace(preffixToReplace, ""));
          }
        }
      } catch (Exception e) {
        logger.error("Completion error", e);
      }

    }
    return completions;
  }

  private int getLastWhitespaceIndex(String s) {
    if (StringUtils.isBlank(s)) {
      return 0;
    }

    for (int i = s.length() - 1; i >= 0; i--) {
      if (Character.isWhitespace(s.charAt(i))) {
        return i;
      }
    }
    return 0;
  }

  private Pair<String, Integer> getCursorLine(String buf, int cursor) {

    logger.info("In Buffer:" + buf.replace(LINE_SEPARATOR, "_") + ":" + cursor);

    String[] lines = buf.split(LINE_SEPARATOR);

    int processedSize = 0;
    for (String line : lines) {
      int lineLength = line.length() + LINE_SEPARATOR.length();
      logger.info(line + ":" + lineLength + " : " + (processedSize + line.length()));
      if (cursor <= (processedSize + lineLength)) {
        return new ImmutablePair<String, Integer>(line, cursor - processedSize);
      }
      processedSize += lineLength;
    }

    return new ImmutablePair<String, Integer>("", 0);
  }
}

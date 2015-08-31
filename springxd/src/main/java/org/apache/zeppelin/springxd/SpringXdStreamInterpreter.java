package org.apache.zeppelin.springxd;

import static org.apache.commons.lang.StringUtils.isBlank;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.AngularObjectWatcher;
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

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;


/**
 * SpringXD interpreter for Zeppelin.
 * 
 * <ul>
 * <li>{@code springxd.url} - JDBC URL to connect to.</li>
 * </ul>
 * 
 * <p>
 * How to use: <br/>
 * {@code %xd.stream} <br/>
 * {@code 
 *  tweets = twittersearch --query=Obama --outputType=application/json | gemfire-json-server 
 *    --useLocator=true --host=ambari.localdomain --port=10334 
 *    --regionName=regionTweet --keyExpression=payload.getField('id_str')
 *  tweetsCount = tap:stream:tweets > json-to-tuple | transform --expression='payload.id_str' 
 *    | counter --name=tweetCounter
 * }
 * </p>
 *
 */
public class SpringXdStreamInterpreter extends Interpreter {

  private Logger logger = LoggerFactory.getLogger(SpringXdStreamInterpreter.class);

  private static final Pattern NAMED_STREAM_PATTERN = Pattern.compile("\\s*(\\w*)\\s*=\\s*(.*)");
  private static final int SINGLE_LEVEL_OF_DETAILS = 1;
  private static final boolean DEPLOY = true;

  /**
   * Defines the deployed streams status.
   */
  public enum StreamsStatus {
    DEPLOYED, DESTROYED
  };

  public static final String SPRINGXD_URL = "springxd.url";
  public static final String DEFAULT_SPRINGXD_URL = "http://ambari.localdomain:9393";

  private Map<String, Map<String, List<String>>> noteParagraphToDeployedStreamNames;

  private Exception exceptionOnConnect;

  private SpringXDTemplate xdTemplate;

  static {
    Interpreter.register(
        "stream",
        "xd",
        SpringXdStreamInterpreter.class.getName(),
        new InterpreterPropertyBuilder().add(SPRINGXD_URL, DEFAULT_SPRINGXD_URL,
            "The URL for SpringXD REST API.").build());
  }

  public SpringXdStreamInterpreter(Properties property) {
    super(property);
    // deployedStreams = new ArrayList<String>();

    noteParagraphToDeployedStreamNames = new HashMap<String, Map<String, List<String>>>();
  }

  private void addStream(String noteId, String paragraphId, String streamName) {

    if (!noteParagraphToDeployedStreamNames.containsKey(noteId)) {
      noteParagraphToDeployedStreamNames.put(noteId, new HashMap<String, List<String>>());
    }
    if (!noteParagraphToDeployedStreamNames.get(noteId).containsKey(paragraphId)) {
      noteParagraphToDeployedStreamNames.get(noteId).put(paragraphId, new ArrayList<String>());
    }
    noteParagraphToDeployedStreamNames.get(noteId).get(paragraphId).add(streamName);
  }

  private List<String> getStreamsByNotebookParagraph(String noteId, String paragraphId) {

    if (noteParagraphToDeployedStreamNames.containsKey(noteId)
        && noteParagraphToDeployedStreamNames.get(noteId).containsKey(paragraphId)) {
      return noteParagraphToDeployedStreamNames.get(noteId).get(paragraphId);
    }
    return new ArrayList<String>();
  }

  private void destroyStreamsByNotebookParagraph(String noteId, String paragraphId) {
    if (noteParagraphToDeployedStreamNames.containsKey(noteId)
        && noteParagraphToDeployedStreamNames.get(noteId).containsKey(paragraphId)) {

      Iterator<String> it =
          noteParagraphToDeployedStreamNames.get(noteId).get(paragraphId).iterator();

      while (it.hasNext()) {
        String streamName = it.next();
        try {
          xdTemplate.streamOperations().destroy(streamName);
          logger.info("Destroyed :" + streamName + " from [" + noteId + ":" + paragraphId + "]");
          it.remove();
        } catch (Exception e) {
          logger.error("Failed to destroy stream: " + streamName, Throwables.getRootCause(e));
        }
      }
    }
  }

  private void destroyStreamsByNotebook(String noteId) {
    if (noteParagraphToDeployedStreamNames.containsKey(noteId)) {
      Iterator<String> it = noteParagraphToDeployedStreamNames.get(noteId).keySet().iterator();
      while (it.hasNext()) {
        String paragraphId = it.next();
        destroyStreamsByNotebookParagraph(noteId, paragraphId);
      }
    }
  }

  private void destroyStreamsAll() {
    if (!CollectionUtils.isEmpty(noteParagraphToDeployedStreamNames.keySet())) {
      Iterator<String> it = noteParagraphToDeployedStreamNames.keySet().iterator();
      while (it.hasNext()) {
        String noteId = it.next();
        destroyStreamsByNotebook(noteId);
      }
    }
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
    destroyStreamsAll();
  }

  @Override
  public InterpreterResult interpret(String multiLineStreamDefinitions, InterpreterContext ctx) {

    if (exceptionOnConnect != null) {
      return new InterpreterResult(Code.ERROR, exceptionOnConnect.getMessage());
    }

    // (Re)deploying streams means that any previous instances (created by the same
    // notebook/paragraph) will be destroyed
    destroyStreamsByNotebookParagraph(ctx.getNoteId(), ctx.getParagraphId());

    String errorMessage = "";
    try {
      if (!isBlank(multiLineStreamDefinitions)) {
        for (String line : multiLineStreamDefinitions.split(CompletionUtils.LINE_SEPARATOR)) {

          Pair<String, String> namedStream = getNamedStream(line);
          String streamName = namedStream.getLeft();
          String streamDefinition = namedStream.getRight();

          if (!isBlank(streamName) && !isBlank(streamDefinition)) {

            StreamDefinitionResource stream =
                xdTemplate.streamOperations().createStream(streamName, streamDefinition, DEPLOY);

            addStream(ctx.getNoteId(), ctx.getParagraphId(), streamName);

            logger.info("Deployed Stream: [" + streamName + "]:[" + streamDefinition + "]");
          } else {
            logger.info("Skipped Line:" + line);
          }
        }
      }

      String streamStatusId = "streamsStatus_" + ctx.getParagraphId().replace("-", "_");
      // Use the Angualr response to hook a streams destroy button
      angularBind(ctx, streamStatusId, StreamsStatus.DEPLOYED.name(), ctx.getNoteId(),
          new StreamWatcher(ctx));

      List<String> deployedStreams =
          getStreamsByNotebookParagraph(ctx.getNoteId(), ctx.getParagraphId());
      String angularStreamsStatusButton =
          String.format("%%angular <button ng-click='%s = \"%s\"'> "
              + "Streams [%s] : {{%s}} </button>", streamStatusId, StreamsStatus.DESTROYED.name(),
              Joiner.on(", ").join(deployedStreams).toString(), streamStatusId);

      logger.info(angularStreamsStatusButton);

      return new InterpreterResult(Code.SUCCESS, angularStreamsStatusButton);

    } catch (Exception e) {
      logger.error("Failed to deploy streams!", e);
      errorMessage = Throwables.getRootCause(e).getMessage();
      destroyStreamsByNotebookParagraph(ctx.getNoteId(), ctx.getParagraphId());
    }

    return new InterpreterResult(Code.ERROR, "Failed to deploy Streams: " + errorMessage);
  }

  /**
   * Split the input line into a stream name and a stream definition parts, divided by the '='
   * character.
   * 
   * @param line Input line in the form" <stream name> = <stream definition>
   * @return Returns a (<stream name>, <stream definition>) pair. It returns an empty pair ("", "")
   *         if the input line doesn't confirm to the <name> = <definition> convention.
   */
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
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public List<String> completion(String buf, int cursor) {
    List<String> zeppelinCompletions = null;

    if (!isBlank(buf)) {
      try {

        String searchPreffix = CompletionUtils.getSearchPreffix(buf, cursor);

        List<String> xdCompletions =
            xdTemplate.completionOperations().completions(CompletionKind.stream, searchPreffix,
                SINGLE_LEVEL_OF_DETAILS);

        zeppelinCompletions =
            CompletionUtils.convertXdToZeppelinCompletions(xdCompletions, searchPreffix);

      } catch (Exception e) {
        logger.error("Completion error", e);
      }

    }
    return zeppelinCompletions;
  }

  private class StreamWatcher extends AngularObjectWatcher {

    public StreamWatcher(InterpreterContext context) {
      super(context);
    }

    @Override
    public void watch(Object oldObject, Object newObject, InterpreterContext context) {

      logger.info("Angular Event: from [" + oldObject + "] to [" + newObject + "]");

      if (StreamsStatus.DESTROYED.name().equals("" + newObject)) {
        destroyStreamsByNotebookParagraph(context.getNoteId(), context.getParagraphId());
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void angularBind(InterpreterContext context, String name, Object o, String noteId,
      AngularObjectWatcher watcher) {

    AngularObjectRegistry registry = context.getAngularObjectRegistry();

    if (registry.get(name, noteId) == null) {
      registry.add(name, o, noteId);
    } else {
      registry.get(name, noteId).set(o);
    }

    if (registry.get(name, noteId) != null) {
      registry.get(name, noteId).addWatcher(watcher);
      logger.info("Register my watcher!");
    }
  }
}

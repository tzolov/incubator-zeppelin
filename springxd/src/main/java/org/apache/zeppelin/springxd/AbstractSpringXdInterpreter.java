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

import static org.apache.commons.lang.StringUtils.isBlank;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.zeppelin.display.AngularObjectWatcher;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.springxd.AngularBinder.ResourceStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.xd.rest.client.impl.SpringXDTemplate;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;


/**
 * Common SpringXD interpreter for Zeppelin.
 */
public abstract class AbstractSpringXdInterpreter extends Interpreter {

  private Logger logger = LoggerFactory.getLogger(AbstractSpringXdInterpreter.class);

  public static final String SPRINGXD_URL = "springxd.url";
  public static final String DEFAULT_SPRINGXD_URL = "http://ambari.localdomain:9393";

  private Exception exceptionOnConnect;

  private SpringXDTemplate xdTemplate;

  private DeployedResourcesManager deployedResourcesManager;

  private ResourceCompletion resourceCompletion;

  public AbstractSpringXdInterpreter(Properties property) {
    super(property);
  }

  /**
   * @return Returns a Resource (Stream or Job) specific completion implementation
   */
  public abstract ResourceCompletion doCreateResourceCompletion();

  /**
   * @return Returns a Resource (stream or job) specific deployed resource manager.
   */
  public abstract DeployedResourcesManager doCreateDeployedResourcesManager();


  @Override
  public void open() {
    // Destroy any previously deployed resources
    close();
    try {
      String springXdUrl = getProperty(SPRINGXD_URL);
      xdTemplate = new SpringXDTemplate(new URI(springXdUrl));
      deployedResourcesManager = doCreateDeployedResourcesManager();
      resourceCompletion = doCreateResourceCompletion();
      exceptionOnConnect = null;
    } catch (URISyntaxException e) {
      logger.error("Failed to connect to the SpringXD cluster", e);
      exceptionOnConnect = e;
      close();
    }
  }

  @Override
  public void close() {
    if (deployedResourcesManager != null) {
      deployedResourcesManager.destroyAllNotebookDeployedResources();
    }
  }

  @Override
  public InterpreterResult interpret(String multiLineResourceDefinitions, InterpreterContext ctx) {

    if (exceptionOnConnect != null) {
      return new InterpreterResult(Code.ERROR, exceptionOnConnect.getMessage());
    }

    // (Re)deploying jobs means that any previous instances (created by the same
    // notebook/paragraph) will be destroyed
    deployedResourcesManager.destroyDeployedResourceBy(ctx.getNoteId(), ctx.getParagraphId());

    String errorMessage = "";
    try {
      if (!isBlank(multiLineResourceDefinitions)) {
        for (String line : multiLineResourceDefinitions.split(ResourceCompletion.LINE_SEPARATOR)) {

          Pair<String, String> namedDefinition = NamedDefinitionParser.getNamedDefinition(line);

          String resourceName = namedDefinition.getLeft();
          String resourceDefinition = namedDefinition.getRight();

          if (!isBlank(resourceName) && !isBlank(resourceDefinition)) {

            deployedResourcesManager.deployResource(ctx.getNoteId(), ctx.getParagraphId(),
                resourceName, resourceDefinition);

            logger.info("Deployed: [" + resourceName + "]:[" + resourceDefinition + "]");
          } else {
            logger.info("Skipped Line:" + line);
          }
        }
      }

      String angularDestroyButton = createDestroyButton(ctx);

      logger.info(angularDestroyButton);

      return new InterpreterResult(Code.SUCCESS, angularDestroyButton);

    } catch (Exception e) {
      logger.error("Failed to deploy xd resource!", e);
      errorMessage = Throwables.getRootCause(e).getMessage();
      deployedResourcesManager.destroyDeployedResourceBy(ctx.getNoteId(), ctx.getParagraphId());
    }

    return new InterpreterResult(Code.ERROR, "Failed to deploy XD resource: " + errorMessage);
  }

  private String createDestroyButton(InterpreterContext ctx) {

    // Use the Angualr response to hook a resource destroy button
    String resourceStatusId = "resourceStatus_" + ctx.getParagraphId().replace("-", "_");

    AngularBinder.bind(ctx, resourceStatusId, ResourceStatus.DEPLOYED.name(), ctx.getNoteId(),
        new DestroyEventWatcher(ctx));

    List<String> deployedResources =
        deployedResourcesManager.getDeployedResourceBy(ctx.getNoteId(), ctx.getParagraphId());

    String destroyButton =
        String.format("%%angular <button ng-click='%s = \"%s\"'> " + " [%s] : {{%s}} </button>",
            resourceStatusId, ResourceStatus.DESTROYED.name(),
            Joiner.on(", ").join(deployedResources).toString(), resourceStatusId);

    return destroyButton;
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
    return resourceCompletion.completion(buf, cursor);
  }

  private class DestroyEventWatcher extends AngularObjectWatcher {

    public DestroyEventWatcher(InterpreterContext context) {
      super(context);
    }

    @Override
    public void watch(Object oldValue, Object newValue, InterpreterContext context) {
      if (ResourceStatus.DESTROYED.name().equals("" + newValue)) {
        deployedResourcesManager.destroyDeployedResourceBy(context.getNoteId(),
            context.getParagraphId());
      }
    }
  }

  protected SpringXDTemplate getXdTemplate() {
    return xdTemplate;
  }
}

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

/**
 * @author tzoloc
 *
 */
public abstract class AbstractDeployedResourcesManager {

  public static final boolean DEPLOY = true;

  private Logger logger = LoggerFactory.getLogger(AbstractDeployedResourcesManager.class);

  private Map<String, Map<String, List<String>>> note2paragraph2Resources;

  public AbstractDeployedResourcesManager() {
    this.note2paragraph2Resources = new HashMap<String, Map<String, List<String>>>();
  }

  public void deployResource(String noteId, String paragraphId, String resourceName,
      String resourceDefininition) {

    if (!isBlank(resourceName) && !isBlank(resourceDefininition)) {

      doCreateResource(resourceName, resourceDefininition);

      if (!note2paragraph2Resources.containsKey(noteId)) {
        note2paragraph2Resources.put(noteId, new HashMap<String, List<String>>());
      }

      if (!note2paragraph2Resources.get(noteId).containsKey(paragraphId)) {
        note2paragraph2Resources.get(noteId).put(paragraphId, new ArrayList<String>());
      }
      note2paragraph2Resources.get(noteId).get(paragraphId).add(resourceName);
    }
  }

  public List<String> getDeployedResourceBy(String noteId, String paragraphId) {
    if (note2paragraph2Resources.containsKey(noteId)
        && note2paragraph2Resources.get(noteId).containsKey(paragraphId)) {
      return note2paragraph2Resources.get(noteId).get(paragraphId);
    }
    return new ArrayList<String>();
  }

  public void destroyDeployedResourceBy(String noteId, String paragraphId) {
    if (note2paragraph2Resources.containsKey(noteId)
        && note2paragraph2Resources.get(noteId).containsKey(paragraphId)) {

      Iterator<String> it = note2paragraph2Resources.get(noteId).get(paragraphId).iterator();
      while (it.hasNext()) {
        String resourceName = it.next();
        try {

          doDestroyRsource(resourceName);

          logger.info("Destroyed :" + resourceName + " from [" + noteId + ":" + paragraphId + "]");
          it.remove();
        } catch (Exception e) {
          logger.error("Failed to destroy resource: " + resourceName, Throwables.getRootCause(e));
        }
      }
    }
  }

  public void destroyDeployedResourceBy(String noteId) {
    Iterator<String> it = note2paragraph2Resources.keySet().iterator();
    while (it.hasNext()) {
      String paragraphId = it.next();
      destroyDeployedResourceBy(noteId, paragraphId);
    }
  }

  public void destroyAllNotebookDeployedResources() {
    if (!CollectionUtils.isEmpty(note2paragraph2Resources.keySet())) {
      Iterator<String> it = note2paragraph2Resources.keySet().iterator();
      while (it.hasNext()) {
        String noteId = it.next();
        destroyDeployedResourceBy(noteId);
      }
    }
  }

  /**
   * Creates a new Stream or Job SpringXD resource.
   * 
   * @param name Resource (stream or job) name
   * 
   * @param definition Resource (stream or job) definition
   */
  public abstract void doCreateResource(String name, String definition);

  /**
   * Destroys stream or job resource by name
   * 
   * @param name Stream or Job name to destroy.
   */
  public abstract void doDestroyRsource(String name);
}

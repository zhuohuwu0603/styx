/*-
 * -\-\-
 * Spotify End-to-End Integration Tests
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.styx.e2e_tests;

import static com.spotify.styx.e2e_tests.EndToEndTestBase.SCHEDULER_SERVICE_NAME;
import static com.spotify.styx.e2e_tests.TestNamespaces.isOldTestNamespace;
import static java.util.stream.Collectors.toList;

import com.spotify.styx.StyxScheduler;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This removes old kubernetes test namespaces. It is not really a test.
 */
public class KubernetesCleanupTest {

  private static final Logger log = LoggerFactory.getLogger(KubernetesCleanupTest.class);

  @Test
  public void deleteOldKubernetesTestNamespaces() {
    System.setProperty("styx.test.namespace", "dummy");
    var schedulerConfig = ConfigFactory.load(SCHEDULER_SERVICE_NAME);
    var k8s = StyxScheduler.getKubernetesClient(schedulerConfig, "default");

    var oldNamespaces = k8s.namespaces().list().getItems().stream()
        .filter(ns -> isOldTestNamespace(ns.getMetadata().getName()))
        .collect(toList());

    var names = oldNamespaces.stream()
        .map(ns -> ns.getMetadata().getName())
        .collect(toList());

    log.info("Deleting old k8s test namespaces: {}", names);
    k8s.namespaces().delete(oldNamespaces);
  }

}

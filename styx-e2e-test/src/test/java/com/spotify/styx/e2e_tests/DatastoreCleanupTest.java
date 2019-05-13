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

import static com.spotify.styx.e2e_tests.DatastoreUtil.deleteDatastoreNamespace;
import static com.spotify.styx.e2e_tests.EndToEndTestBase.TIMESTAMP_FORMATTER;
import static com.spotify.styx.e2e_tests.TestNamespaces.TEST_NAMESPACE_PATTERN;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.KeyQuery;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This removes old datastore test namespaces. It is not really a test.
 */
public class DatastoreCleanupTest {

  private static final Logger log = LoggerFactory.getLogger(DatastoreCleanupTest.class);

  @Test
  public void deleteOldDatastoreTestNamespaces() {
    final Datastore datastore = DatastoreOptions.newBuilder()
        .setProjectId("styx-oss-test")
        .build()
        .getService();

    var namespaces = new ArrayList<String>();
    datastore.run(KeyQuery.newKeyQueryBuilder().setKind("__namespace__").build())
        .forEachRemaining(k -> {
          if (k.hasName()) {
            namespaces.add(k.getName());
          }
        });
    for (var namespace : namespaces) {
      var matcher = TEST_NAMESPACE_PATTERN.matcher(namespace);
      if (!matcher.matches()) {
        continue;
      }
      final Instant timestamp;
      var timestampString = matcher.group("timestamp");
      try {
        timestamp = TIMESTAMP_FORMATTER.parse(timestampString, Instant::from);
      } catch (Exception e) {
        log.warn("Failed to parse namespace timestamp: " + timestampString, e);
        continue;
      }
      if (timestamp.isAfter(Instant.now().minus(1, ChronoUnit.DAYS))) {
        continue;
      }
      log.info("Deleting old datastore test namespace: {}", namespace);
      deleteDatastoreNamespace(datastore, namespace);
    }
  }

}

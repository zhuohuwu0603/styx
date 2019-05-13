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
import static com.spotify.styx.e2e_tests.TestNamespaces.TEST_NAMESPACE_PREFIX;
import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.google.cloud.datastore.Datastore;
import com.spotify.apollo.core.Service;
import com.spotify.apollo.httpservice.HttpService;
import com.spotify.spawn.Subprocesses;
import com.spotify.styx.StyxApi;
import com.spotify.styx.StyxScheduler;
import com.spotify.styx.cli.CliMain;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.util.Connections;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.norberg.automatter.AutoMatter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import javaslang.control.Try;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndToEndTestBase {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  static final Logger log = LoggerFactory.getLogger(EndToEndTestBase.class);

  static final String SCHEDULER_SERVICE_NAME = "styx-e2e-test-scheduler";
  static final String API_SERVICE_NAME = "styx-e2e-test-api";

  final ExecutorService executor = Executors.newCachedThreadPool();

  static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter
      .ofPattern("yyyyMMdd-HHmmss", Locale.ROOT)
      .withZone(ZoneOffset.UTC);

  final String namespace = String.join("-",
      TEST_NAMESPACE_PREFIX,
      TIMESTAMP_FORMATTER.format(Instant.now()),
      Long.toHexString(ThreadLocalRandom.current().nextLong()));

  final String component1 = namespace + "-c-1";
  final String workflowId1 = namespace + "-wf-1";

  private CompletableFuture<Service.Instance> styxSchedulerInstance = new CompletableFuture<>();
  private CompletableFuture<Service.Instance> styxApiInstance = new CompletableFuture<>();

  private Future<Object> styxSchedulerThread;
  private Future<Object> styxApiThread;

  int styxApiPort = -1;

  private Datastore datastore;
  private NamespacedKubernetesClient k8s;

  private Config schedulerConfig;

  @Before
  public void setUpStyx() throws Exception {
    // Setup namespace
    log.info("Styx test namespace: {}", namespace);
    System.setProperty("styx.test.namespace", namespace);

    schedulerConfig = ConfigFactory.load(SCHEDULER_SERVICE_NAME);
    datastore = Connections.createDatastore(schedulerConfig, Stats.NOOP);

    System.setProperty("kubernetes.auth.tryKubeConfig", "false");
    log.info("Creating k8s namespace: {}", namespace);
    k8s = StyxScheduler.getKubernetesClient(schedulerConfig, "default");
    k8s.namespaces().createNew()
        .withNewMetadata().withName(namespace).endMetadata()
        .done();

    // Start scheduler
    styxSchedulerThread = executor.submit(() -> {
      HttpService.boot(env -> StyxScheduler.newBuilder()
          .setServiceName(SCHEDULER_SERVICE_NAME)
          .build()
          .create(env), SCHEDULER_SERVICE_NAME, styxSchedulerInstance::complete);
      return null;
    });

    // Start api
    styxApiThread = executor.submit(() -> {
      HttpService.boot(env -> StyxApi.newBuilder()
          .setServiceName(API_SERVICE_NAME)
          .build()
          .create(env), API_SERVICE_NAME, styxApiInstance::complete);
      return null;
    });

    await().atMost(300, SECONDS)
        .until(() -> {
          if (styxSchedulerThread.isDone()) {
            styxSchedulerThread.get();
          }
          if (styxApiThread.isDone()) {
            styxApiThread.get();
          }
          return styxSchedulerInstance.isDone() && styxApiInstance.isDone();
        });

    styxApiPort = styxApiInstance.join().getConfig().getInt("http.server.port");
  }

  @After
  public void tearDownStyx() throws Exception {
    styxApiInstance.thenAccept(instance -> instance.getSignaller().signalShutdown());
    styxSchedulerInstance.thenAccept(instance -> instance.getSignaller().signalShutdown());
    if (styxApiThread != null) {
      Try.run(() -> styxApiThread.get(30, SECONDS));
      styxApiThread.cancel(true);
    }
    if (styxSchedulerThread != null) {
      Try.run(() -> styxSchedulerThread.get(30, SECONDS));
      styxSchedulerThread.cancel(true);
    }
    executor.shutdownNow();
    executor.awaitTermination(30, SECONDS);
  }

  @After
  public void datastoreCleanup() {
    if (datastore == null) {
      return;
    }
    deleteDatastoreNamespace(datastore, namespace);
  }

  @After
  public void k8sCleanup() {
    if (k8s == null) {
      return;
    }
    log.info("Deleting k8s namespace: {}", namespace);
    k8s.namespaces().withName(namespace).delete();
  }

  <T> T cliJson(Class<T> outputClass, String... args) throws IOException, InterruptedException, CliException {
    return cliJson(Json.OBJECT_MAPPER.getTypeFactory().constructType(outputClass), List.of(args));
  }

  <T> T cliJson(TypeReference<T> outputType, String... args)
      throws IOException, InterruptedException, CliException {
    return cliJson(Json.OBJECT_MAPPER.getTypeFactory().constructType(outputType), List.of(args));
  }

  <T> T cliJson(JavaType outputType, List<String> args)
      throws IOException, InterruptedException, CliException {
    var jsonArgs = new ArrayList<String>();
    jsonArgs.add("--json");
    jsonArgs.addAll(args);
    var output = cli(jsonArgs);
    return Json.OBJECT_MAPPER.readValue(output, outputType);
  }

  byte[] cli(List<String> args) throws IOException, InterruptedException, CliException {

    var stdout = new ByteArrayOutputStream();

    var spawner = Subprocesses.process().main(CliMain.class)
        .args(args)
        .redirectStderr(INHERIT)
        .pipeStdout(stdout);

    spawner.processBuilder().environment().put("STYX_CLI_HOST", "http://127.0.0.1:" + styxApiPort);

    var process = spawner.spawn();

    var exited = process.process().waitFor(120, SECONDS);
    if (!exited) {
      process.kill();
      throw new AssertionError("cli timeout");
    }

    var exitCode = process.process().exitValue();
    if (exitCode != 0) {
      throw new CliException("cli failed", exitCode);
    }

    return stdout.toByteArray();
  }

  @AutoMatter
  interface WorkflowWithState {

    Workflow workflow();

    WorkflowState state();
  }

  class CliException extends Exception {

    final int code;

    private CliException(String message, int code) {
      super(message + ": code=" + code);
      this.code = code;
    }
  }
}

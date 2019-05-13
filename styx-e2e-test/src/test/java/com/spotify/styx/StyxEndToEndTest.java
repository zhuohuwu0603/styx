package com.spotify.styx;

import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery;
import com.google.common.collect.Lists;
import com.spotify.apollo.AppInit;
import com.spotify.apollo.core.Service;
import com.spotify.apollo.httpservice.HttpService;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.spawn.Subprocesses;
import com.spotify.styx.api.AuthenticatorFactory;
import com.spotify.styx.cli.CliMain;
import com.spotify.styx.model.data.EventInfo;
import com.spotify.styx.monitoring.MetricsStats;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.monitoring.StatsFactory;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.util.Connections;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import javaslang.Function1;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StyxEndToEndTest {

  private static final String TEST_NAMESPACE_PREFIX = "styx-e2e-test";

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final Logger log = LoggerFactory.getLogger(StyxEndToEndTest.class);

  private static final String SERVICE_NAME = "styx-e2e-test";

  private static final ExecutorService executor = Executors.newCachedThreadPool();

  private Future<Object> styxService;
  private String namespace;
  private CompletableFuture<Service.Instance> styxInstance = new CompletableFuture<>();

  private int styxPort = -1;

  private volatile Datastore datastore;
  private volatile NamespacedKubernetesClient k8s;

  @Before
  public void setUpStyx() throws Exception {
    var now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
    this.namespace = String.join("-",
        TEST_NAMESPACE_PREFIX,
        now.toString().replaceAll("[:.\\-Z]", "").replaceAll("T", "-"),
        Long.toHexString(ThreadLocalRandom.current().nextLong()));

    log.info("Styx test namespace: {}", namespace);

    System.setProperty("styx.test.namespace", namespace);
    System.setProperty("kubernetes.auth.tryKubeConfig", "false");

    // TODO: run api and scheduler separately

    // Start styx on a thread
    this.styxService = executor.submit(() -> {

      final AppInit init = env -> {

        this.datastore = Connections.createDatastore(env.config(), Stats.NOOP);
        this.k8s = StyxScheduler.getKubernetesClient(env.config(), "default");

        log.info("Creating k8s namespace: {}", namespace);
        var k8sNamespace = k8s.namespaces().createNew()
            .withNewMetadata().withName(namespace).endMetadata()
            .done();
        log.info("Created k8s namespace: {}", k8sNamespace.getMetadata().getName());

        final MetricsStats stats =
            new MetricsStats(env.resolve(SemanticMetricRegistry.class), Instant::now);
        final StatsFactory statsFactory = (ignored) -> stats;

        final AuthenticatorFactory authenticatorFactory =
            Function1.of(AuthenticatorFactory.DEFAULT::apply).memoized()::apply;

        final StyxScheduler scheduler = StyxScheduler.newBuilder()
            .setServiceName(SERVICE_NAME)
            .setStatsFactory(statsFactory)
            .setAuthenticatorFactory(authenticatorFactory)
            .build();

        final StyxApi api = StyxApi.newBuilder()
            .setServiceName(SERVICE_NAME)
            .setStatsFactory(statsFactory)
            .setAuthenticatorFactory(authenticatorFactory)
            .build();

        scheduler.create(env);
        api.create(env);
      };

      HttpService.boot(init, SERVICE_NAME, styxInstance::complete);

      return null;
    });

    // Wait for styx to boot
    var styx = styxInstance.get(300, SECONDS);
    styxPort = styx.getConfig().getInt("http.server.port");
  }

  @After
  public void tearDownStyx() throws Exception {
    if (styxInstance.isDone()) {
      styxInstance.join().getSignaller().signalShutdown();
    }
    styxService.cancel(true);
    executor.shutdownNow();
    executor.awaitTermination(30, SECONDS);
  }

  @After
  public void datastoreCleanup() {
    // TODO: remove old test namespaces
    var keys = datastore.run(KeyQuery.newKeyQueryBuilder().build());
    var allKeys = new ArrayList<Key>();
    log.info("Deleting datastore entities: {}", allKeys.size());
    keys.forEachRemaining(allKeys::add);
    Lists.partition(allKeys, 500).forEach(keyBatch ->
        datastore.delete(keyBatch.toArray(Key[]::new)));
  }

  @After
  public void k8sCleanup() throws Exception {
    // TODO: remove old test namespaces
    var pods = k8s.pods().list().getItems();
    if (pods.isEmpty()) {
      return;
    }
    log.info("Deleting pods: {}", pods.size());
    k8s.pods().delete(pods);
    log.info("Deleting k8s namespace: {}", namespace);
    k8s.namespaces().withName(namespace).delete();
  }

  @Test
  public void testEndToEnd() throws IOException, InterruptedException {

    // Generate workflow configuration
    var component = namespace + "-c-1";
    var workflowId = namespace + "-wf-1";
    var workflowJson = Json.OBJECT_MAPPER.writeValueAsString(Map.of(
        "id", workflowId,
        "schedule", "daily",
        "docker_image", "busybox",
        "docker_args", List.of("echo", "hello world")));
    var workflowJsonFile = temporaryFolder.newFile().toPath();
    Files.writeString(workflowJsonFile, workflowJson);

    // Create workflow
    log.info("Creating workflow: {}", workflowId);
    var workflowCreateResult = cliJson(String.class,
        "workflow", "create", "-f", workflowJsonFile.toString(), component);
    assertThat(workflowCreateResult, is("Workflow " + workflowId + " in component " + component + " created."));

    // Trigger workflow instance
    log.info("Triggering workflow");
    var instance = "2019-05-13";
    var triggerResult = cliJson(String.class, "t", component, workflowId, instance);
    assertThat(triggerResult, is("Triggered! Use `styx ls -c " + component + "` to check active workflow instances."));

    // Wait for instance to successfully complete
    await().atMost(5, MINUTES).until(() -> {
      var events = cliJson(new TypeReference<List<EventInfo>>() {}, "e", component, workflowId, instance);
      return events.stream().anyMatch(event -> event.name().equals("success"));
    });

    // Delete workflow
    var deleteResult = cliJson(String.class, "workflow", "delete", "--force", component, workflowId);
    assertThat(deleteResult, is("Workflow " + workflowId + " in component " + component + " deleted."));
  }

  private <T> T cliJson(Class<T> outputClass, String... args) throws IOException, InterruptedException {
    return cliJson(Json.OBJECT_MAPPER.getTypeFactory().constructType(outputClass), List.of(args));
  }

  private <T> T cliJson(TypeReference<T> outputType, String... args) throws IOException, InterruptedException {
    return cliJson(Json.OBJECT_MAPPER.getTypeFactory().constructType(outputType), List.of(args));
  }

  private <T> T cliJson(JavaType outputType, List<String> args)
      throws IOException, InterruptedException {
    var jsonArgs = new ArrayList<String>();
    jsonArgs.add("--json");
    jsonArgs.addAll(args);
    var output = cli(jsonArgs);
    return Json.OBJECT_MAPPER.readValue(output, outputType);
  }

  private byte[] cli(List<String> args) throws IOException, InterruptedException {

    var stdout = new ByteArrayOutputStream();

    var spawner = Subprocesses.process().main(CliMain.class)
        .args(args)
        .redirectStderr(INHERIT)
        .pipeStdout(stdout);

    spawner.processBuilder().environment().put("STYX_CLI_HOST", "http://127.0.0.1:" + styxPort);

    var process = spawner.spawn();

    var exited = process.process().waitFor(120, SECONDS);
    if (!exited) {
      process.kill();
      throw new AssertionError("cli timeout");
    }

    var exitCode = process.process().exitValue();
    if (exitCode != 0) {
      throw new AssertionError("cli failed: exit code = " + exitCode);
    }

    return stdout.toByteArray();
  }
}

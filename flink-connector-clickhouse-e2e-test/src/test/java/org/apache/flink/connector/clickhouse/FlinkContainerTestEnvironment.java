package org.apache.flink.connector.clickhouse;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.util.Preconditions.checkState;

/** Test environment running job on Flink containers. */
public class FlinkContainerTestEnvironment {

    private static final Logger logger =
            LoggerFactory.getLogger(FlinkContainerTestEnvironment.class);
    public static final Network NETWORK = Network.newNetwork();

    static final ClickHouseContainer CLICKHOUSE_CONTAINER =
            new ClickHouseContainer("clickhouse/clickhouse-server:latest")
                    .withNetwork(NETWORK)
                    .withNetworkAliases("clickhouse")
                    .withExposedPorts(8123, 9000)
                    .withUsername("test_username")
                    .withPassword("test_password")
                    .withLogConsumer(new Slf4jLogConsumer(logger));

    private static final TestcontainersSettings TESTCONTAINERS_SETTINGS =
            TestcontainersSettings.builder()
                    .logger(logger)
                    .network(NETWORK)
                    .dependsOn(CLICKHOUSE_CONTAINER)
                    .build();

    public static final Path SQL_CONNECTOR_CLICKHOUSE_JAR =
            ResourceTestUtils.getResource("flink-connector-clickhouse-1.0.0-SNAPSHOT.jar");
    public static final Path CLICKHOUSE_JDBC_JAR =
            ResourceTestUtils.getResource("clickhouse-jdbc-0.6.4.jar");
    public static final Path HTTPCORE_JAR = ResourceTestUtils.getResource("httpcore5-5.2.jar");
    public static final Path HTTPCLIENT_JAR =
            ResourceTestUtils.getResource("httpclient5-5.2.1.jar");
    public static final Path HTTPCLIENT_H2_JAR =
            ResourceTestUtils.getResource("httpcore5-h2-5.2.jar");
    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private GenericContainer<?> jobManager;
    private GenericContainer<?> taskManager;
    protected RestClusterClient<StandaloneClusterId> restClusterClient;

    @Before
    public void setUp() throws Exception {
        CLICKHOUSE_CONTAINER.start();

        String properties =
                String.join(
                        "\n",
                        Arrays.asList(
                                "jobmanager.rpc.address: jobmanager",
                                "heartbeat.timeout: 60000",
                                "parallelism.default: 1"));
        jobManager =
                new GenericContainer<>(DockerImageName.parse("flink:1.19.0-scala_2.12"))
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withExtraHost("host.docker.internal", "host-gateway")
                        .withNetworkAliases("jobmanager")
                        .withExposedPorts(8081, 6123)
                        .dependsOn(CLICKHOUSE_CONTAINER)
                        .withLabel("com.testcontainers.allow-filesystem-access", "true")
                        .withEnv("FLINK_PROPERTIES", properties)
                        .withLogConsumer(new Slf4jLogConsumer(logger));
        taskManager =
                new GenericContainer<>(DockerImageName.parse("flink:1.19.0-scala_2.12"))
                        .withCommand("taskmanager")
                        .withExtraHost("host.docker.internal", "host-gateway")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("taskmanager")
                        .withEnv("FLINK_PROPERTIES", properties)
                        .dependsOn(jobManager)
                        .withLabel("com.testcontainers.allow-filesystem-access", "true")
                        .withLogConsumer(new Slf4jLogConsumer(logger));
        Startables.deepStart(Stream.of(jobManager)).join();
        Startables.deepStart(Stream.of(taskManager)).join();
        Thread.sleep(5000);
        logger.info("Containers are started.");
    }

    /**
     * Returns the {@link RestClusterClient} for the running cluster.
     *
     * <p><b>NOTE:</b> The client is created lazily and should only be retrieved after the cluster
     * is running.
     */
    public RestClusterClient<StandaloneClusterId> getRestClusterClient() {
        if (restClusterClient != null) {
            return restClusterClient;
        }
        checkState(
                jobManager.isRunning(),
                "Cluster client should only be retrieved for a running cluster");
        try {
            final Configuration clientConfiguration = new Configuration();
            clientConfiguration.set(RestOptions.ADDRESS, jobManager.getHost());
            clientConfiguration.set(RestOptions.PORT, jobManager.getMappedPort(8081));
            this.restClusterClient =
                    new RestClusterClient<>(clientConfiguration, StandaloneClusterId.getInstance());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to create client for Flink container cluster", e);
        }
        return restClusterClient;
    }

    /**
     * Submits a SQL job to the running cluster.
     *
     * <p><b>NOTE:</b> You should not use {@code '\t'}.
     */
    public void submitSQLJob(List<String> sqlLines, Path... jars)
            throws IOException, InterruptedException {
        logger.info("submitting flink sql task");

        SQLJobSubmission job =
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines).addJars(jars).build();
        final List<String> commands = new ArrayList<>();
        Path script = temporaryFolder.newFile().toPath();
        Files.write(script, job.getSqlLines());
        jobManager.copyFileToContainer(MountableFile.forHostPath(script), "/tmp/script.sql");
        commands.add("cat /tmp/script.sql | ");
        commands.add("bin/sql-client.sh");
        for (String jar : job.getJars()) {
            commands.add("--jar");
            String containerPath = copyAndGetContainerPath(jobManager, jar);
            commands.add(containerPath);
        }

        Container.ExecResult execResult =
                jobManager.execInContainer("bash", "-c", String.join(" ", commands));
        logger.info("execute result:" + execResult.getStdout());
        logger.error("execute error:" + execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when submitting the SQL job.");
        }
    }

    /*
     * Copy the file to the container and return the container path.
     */
    private String copyAndGetContainerPath(GenericContainer<?> container, String filePath) {
        Path path = Paths.get(filePath);
        String containerPath = "/tmp/" + path.getFileName();
        container.copyFileToContainer(MountableFile.forHostPath(path), containerPath);
        return containerPath;
    }

    private static List<String> readSqlFile(final String resourceName) throws Exception {
        return Files.readAllLines(
                Paths.get(ClickhouseE2ECase.class.getResource("/" + resourceName).toURI()));
    }

    public void waitUntilJobRunning(Duration timeout) {
        RestClusterClient<?> clusterClient = getRestClusterClient();
        Deadline deadline = Deadline.fromNow(timeout);
        while (deadline.hasTimeLeft()) {
            Collection<JobStatusMessage> jobStatusMessages;
            try {
                jobStatusMessages = clusterClient.listJobs().get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                logger.warn("Error when fetching job status.", e);
                continue;
            }
            if (jobStatusMessages != null && !jobStatusMessages.isEmpty()) {
                JobStatusMessage message = jobStatusMessages.iterator().next();
                JobStatus jobStatus = message.getJobState();
                if (jobStatus.isTerminalState() && message.getJobState() == JobStatus.FAILED) {
                    throw new ValidationException(
                            String.format(
                                    "Job has been terminated! JobName: %s, JobID: %s, Status: %s",
                                    message.getJobName(),
                                    message.getJobId(),
                                    message.getJobState()));
                } else if (jobStatus == JobStatus.RUNNING) {
                    return;
                }
            }
        }
    }
}

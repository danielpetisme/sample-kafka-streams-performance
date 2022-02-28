package com.examples.danielpetisme;

import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.To;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Hello world!
 */
public class SampleProcessedRecords {

    private static final String KAFKA_ENV_PREFIX = "KAFKA_";
    private final Logger logger = LoggerFactory.getLogger(SampleProcessedRecords.class);
    private final KafkaStreams streams;
    private final Topics topics;
    PrometheusMeterRegistry prometheusRegistry;

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        new SampleProcessedRecords(Collections.emptyMap()).start();
    }

    public SampleProcessedRecords(Map<String, String> config) throws InterruptedException, ExecutionException {
        var properties = buildProperties(defaultProps, System.getenv(), KAFKA_ENV_PREFIX, config);
        AdminClient adminClient = KafkaAdminClient.create(properties);
        this.topics = new Topics(adminClient);
        prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        var topology = buildTopology();
        logger.info(topology.describe().toString());
        logger.info("creating streams with props: {}", properties);
        streams = new KafkaStreams(topology, properties);
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));


        new KafkaStreamsMetrics(this.streams).bindTo(prometheusRegistry);

        int port = Integer.parseInt(System.getenv().getOrDefault("PROMETHEUS_PORT", "8080"));
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/metrics", httpExchange -> {
                String response = prometheusRegistry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });

            server.createContext("/topology", httpExchange -> {
                String response = topology.describe().toString();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = httpExchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            });

            new Thread(server::start).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    Topology buildTopology() {

        var builder = new StreamsBuilder();
        var input = builder.stream(
                topics.inputTopic,
                Consumed.with(Serdes.String(), Serdes.String()));

        input
                .transform(() -> new TimestampDisplayTransformer())
                .transform(() -> new CustomLatencyTransformer())
                .mapValues(v -> v + "_mapped")
                .repartition()
                .mapValues(v -> v + "_repartitioned")
                .peek((k, v) -> logger.info("Post-latency 1_0 -  K: " + k, ", V: " + v))
                .transform(() -> new CustomLatencyTransformer())
                .transform(() -> new TimestampDisplayTransformer())
                .to(topics.outputTopic);

        return builder.build();

    }

    static Map<String, Long> samples = new HashMap<>();

    private class CustomLatencyTransformer implements Transformer<String, String, KeyValue<String, String>> {

        Timer timer = Timer
                .builder("custom.timer")
                .publishPercentiles(0.5, 0.95)
                .publishPercentileHistogram()
                .register(prometheusRegistry);

        ProcessorContext context;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {
            if (!samples.containsKey(key)) {
                logger.info("CustomLatencyTransformer {} -  K: {}, V: {}", context.taskId(), key, value);
                samples.put(key, System.currentTimeMillis());
            } else {
                var latency = System.currentTimeMillis() - samples.get(key);
                logger.info("CustomLatencyTransformer {} -  K: {}, V: {}, Latency: {}ms", context.taskId(), key, value, latency);
                timer.record(latency, TimeUnit.MILLISECONDS);
                samples.remove(key);
            }
            context.forward(key, value, To.all().withTimestamp(context.timestamp()));
            return null;
        }

        @Override
        public void close() {

        }
    }

    private class TimestampDisplayTransformer implements Transformer<String, String, KeyValue<String, String>> {

        ProcessorContext context;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {
            logger.info("TimestampDisplayTransformer {} -  K: {}, V: {}, TS: {}", context.taskId(), key, value, context.timestamp());
            return new KeyValue<>(key, value);

        }

        @Override
        public void close() {

        }
    }


    public void start() {
        logger.info("Kafka Streams started");
        streams.start();
    }

    public void stop() {
        streams.close(Duration.ofSeconds(3));
        logger.info("Kafka Streams stopped");
    }

    public static class MyEventTimeExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            return Instant.ofEpochMilli(record.timestamp()).minus(10, ChronoUnit.MINUTES).toEpochMilli();
        }
    }

    private Map<String, String> defaultProps = Map.of(
            StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, Sensor.RecordingLevel.TRACE.name,
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:49881",
            StreamsConfig.APPLICATION_ID_CONFIG, System.getenv().getOrDefault("APPLICATION_ID", SampleProcessedRecords.class.getName()),
            StreamsConfig.REPLICATION_FACTOR_CONFIG, "1",
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName(),
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName(),
            StreamsConfig.consumerPrefix(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG), "my-app",
            StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest"
    );

    private Properties buildProperties(Map<String, String> baseProps, Map<String, String> envProps, String prefix, Map<String, String> customProps) {
        Map<String, String> systemProperties = envProps.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith(prefix))
                .collect(Collectors.toMap(
                        e -> e.getKey()
                                .replace(prefix, "")
                                .toLowerCase()
                                .replace("_", ".")
                        , e -> e.getValue())
                );

        Properties props = new Properties();
        props.putAll(baseProps);
        props.putAll(systemProperties);
        props.putAll(customProps);
        return props;
    }
}

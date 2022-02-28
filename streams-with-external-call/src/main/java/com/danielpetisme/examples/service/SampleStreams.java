package com.danielpetisme.examples.service;

import com.danielpetisme.examples.config.SampleStreamsConfig;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
public class SampleStreams {

    private static final Logger logger = LoggerFactory.getLogger(SampleStreams.class);

    private final MeterRegistry meterRegistry;
    private final MachineLearningService machineLearningService;
    private final SampleStreamsConfig sampleStreamsConfig;
    public final Topology topology;
    private KafkaStreams streams;

    public SampleStreams(MachineLearningService machineLearningService, SampleStreamsConfig sampleStreamsConfig, MeterRegistry meterRegistry) {
        this.machineLearningService = machineLearningService;
        this.sampleStreamsConfig = sampleStreamsConfig;
        this.meterRegistry = meterRegistry;
        this.topology = buildTopology();
    }

    /**
     * This starts the streams application
     */
    @PostConstruct
    public void start() {
        logger.info("Starting Sample Streams App");
        streams = new KafkaStreams(topology, sampleStreamsConfig.getProperties());
        new KafkaStreamsMetrics(streams).bindTo(meterRegistry);
//		streams.cleanUp(); use this only in dev as this will clean up all state stores

        // This ensures any uncaught exceptions would allow the stream thread to be recreated.
        streams.setStateListener((newState, oldState) -> {
            logger.info("Passing from state: {} to {}", newState, oldState);
        });
        streams.setUncaughtExceptionHandler(ex -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD);
        streams.start();
    }

    @PreDestroy
    public void stop() {
        logger.info("Stopping Greetings Streams App and cleaning up");
        if (streams != null)
            streams.close(Duration.ofMinutes(5));

    }

    private Topology buildTopology() {
        var builder = new StreamsBuilder();
        builder.stream(sampleStreamsConfig.getInputTopic(), Consumed.with(Serdes.String(), Serdes.String()))
                .transform(() -> new CustomLatencyTransformer<>())
                .mapValues(value -> Integer.valueOf(value))
                .mapValues(value -> {
                    int predicted = -1;
                    try {
                        logger.info("0_0 Predicting value for {}", value);
                        predicted = machineLearningService.predict(value);
                    } catch (Exception e) {
                        logger.error("Prediction failed step1: {}", e.getMessage());
                    }
                    return predicted;

                })
                .transform(() -> new CustomLatencyTransformer<>())
                .to(
                        sampleStreamsConfig.getOutputTopic(),
                        Produced.with(Serdes.String(), Serdes.Integer())
                );

        return builder.build();
    }

    static Map<String, Long> samples = new HashMap<>();

    private class CustomLatencyTransformer<V> implements Transformer<String, V, KeyValue<String, V>> {

        Timer timer = Timer
                .builder("custom.timer")
                .publishPercentiles(0.5, 0.95)
                .publishPercentileHistogram()
                .register(meterRegistry);

        ProcessorContext context;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
        }

        @Override
        public KeyValue<String, V> transform(String key, V value) {
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
}

package com.danielpetisme.examples.service;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@Service
public class MachineLearningService {

    private static final Logger logger = LoggerFactory.getLogger(MachineLearningService.class);
    private final Random random = new Random();

    public int durationMs = 0;
    public int percentageOfFailures = 0;

    public int predict(int value) throws Exception {
        logger.info("Simulating a call to a Machine Learning system that will take {}ms for message {}", durationMs, value);
        TimeUnit.MILLISECONDS.sleep(durationMs);
        if (random.nextInt(100) < percentageOfFailures) {
            throw new Exception("Call to external system failed");
        }
        return value;
    }


}


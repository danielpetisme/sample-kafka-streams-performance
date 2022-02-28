package com.danielpetisme.examples.web;

import com.danielpetisme.examples.service.MachineLearningService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class MachineLearningConfigurationController {

    private final MachineLearningService machineLearningService;

    public MachineLearningConfigurationController(MachineLearningService machineLearningService) {
        this.machineLearningService = machineLearningService;
    }

    @GetMapping("/machine-learning/configuration")
    public MachineLearningConfiguration getConfiguration() {
        return new MachineLearningConfiguration(machineLearningService.durationMs, machineLearningService.percentageOfFailures);
    }

    @GetMapping("/machine-learning/configuration/duration/{duration}")
    public ResponseEntity setDuration(@PathVariable(value = "duration") int duration) {
        machineLearningService.durationMs = duration;
        return ResponseEntity.ok(
                new MachineLearningConfiguration(machineLearningService.durationMs, machineLearningService.percentageOfFailures)
        );
    }

    @GetMapping("/machine-learning/configuration/percentageOfFailures/{percentageOfFailures}")
    public ResponseEntity setPercentageOfFailures(@PathVariable(value = "percentageOfFailures") int percentageOfFailures) {
        machineLearningService.percentageOfFailures = percentageOfFailures;
        return ResponseEntity.ok(
                new MachineLearningConfiguration(machineLearningService.durationMs, machineLearningService.percentageOfFailures)
        );
    }

    static class MachineLearningConfiguration {
        private final int duration;
        private final int percentageOfFailures;

        public MachineLearningConfiguration(int duration, int percentageOfFailures) {
            this.duration = duration;
            this.percentageOfFailures = percentageOfFailures;
        }

        public int getDuration() {
            return duration;
        }

        public int getPercentageOfFailures() {
            return percentageOfFailures;
        }
    }
}

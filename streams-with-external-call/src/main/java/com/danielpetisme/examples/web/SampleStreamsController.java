package com.danielpetisme.examples.web;

import com.danielpetisme.examples.service.SampleStreams;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class SampleStreamsController {

    private final SampleStreams sampleStreams;

    public SampleStreamsController(SampleStreams sampleStreams) {
        this.sampleStreams = sampleStreams;
    }

    @GetMapping(value = "/topology", produces = "text/plain")
    public String getTopology() {
        return sampleStreams.topology.describe().toString();
    }
}

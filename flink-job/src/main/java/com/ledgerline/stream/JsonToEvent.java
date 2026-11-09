package com.ledgerline.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/** Kafka value (JSON string) -> {@link Event}. One mapper per subtask. */
public class JsonToEvent extends RichMapFunction<String, Event> {
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();
    }

    @Override
    public Event map(String value) throws Exception {
        return mapper.readValue(value, Event.class);
    }
}

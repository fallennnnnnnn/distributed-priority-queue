package com.queue.distributedpriorityqueue.coordination;

import tools.jackson.databind.json.JsonMapper;

/** How metadata is written into the coordinator's keys: as JSON, so that it can be read with etcdctl. */
final class MetadataJson {

    private static final JsonMapper JSON = JsonMapper.builder().build();

    private MetadataJson() {
    }

    static String write(Object value) {
        return JSON.writeValueAsString(value);
    }

    static <T> T read(String json, Class<T> type) {
        return JSON.readValue(json, type);
    }
}

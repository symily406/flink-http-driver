package com.easy.etl.flink.http.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** {@code POST /v1/sessions}：可选的会话属性（对应 SET 语句）。 */
public final class OpenSessionRequest implements JsonBody {

    private final Map<String, String> properties;

    public OpenSessionRequest(Map<String, String> properties) {
        this.properties = properties == null
                ? Collections.<String, String>emptyMap()
                : Collections.unmodifiableMap(new LinkedHashMap<String, String>(properties));
    }

    public static OpenSessionRequest withoutProperties() {
        return new OpenSessionRequest(null);
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public Map<String, Object> toJson() {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        if (!properties.isEmpty()) {
            body.put("properties", properties);
        }
        return body;
    }

    @Override
    public String toString() {
        return "OpenSessionRequest{properties=" + properties + "}";
    }
}

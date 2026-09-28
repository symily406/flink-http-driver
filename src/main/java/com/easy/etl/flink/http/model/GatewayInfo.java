package com.easy.etl.flink.http.model;

import com.easy.etl.flink.http.util.Json;

import java.util.Map;

/** {@code GET /v1/info}：网关报告的产品名称与版本。 */
public final class GatewayInfo {

    private final String productName;
    private final String version;

    public GatewayInfo(String productName, String version) {
        this.productName = productName;
        this.version = version;
    }

    public static GatewayInfo fromJson(Map<String, Object> json) {
        return new GatewayInfo(Json.getStr(json, "productName"), Json.getStr(json, "version"));
    }

    public String getProductName() {
        return productName;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public String toString() {
        return "GatewayInfo{productName=" + productName + ", version=" + version + "}";
    }
}

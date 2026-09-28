package com.easy.etl.flink.http.gateway;

import java.io.IOException;

/**
 * 当网关操作以非成功的终结状态结束时抛出（ERROR / CANCELED / CLOSED / TIMEOUT），
 * 或在超过客户端超时时间时抛出。
 */
public class GatewayExecutionException extends IOException {

    private static final long serialVersionUID = 1L;

    public GatewayExecutionException(String message) {
        super(message);
    }

    public GatewayExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}

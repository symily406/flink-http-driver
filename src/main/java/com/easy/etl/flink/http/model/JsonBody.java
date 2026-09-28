package com.easy.etl.flink.http.model;

import java.util.Map;

/**
 * 可以被序列化为 JSON 的请求体。
 *
 * <p>客户端发送的每一个 {@code POST} 都被建模为一个 {@code JsonBody}，这样调用方
 * 看到的是类型化的对象，而不是手工构建的 {@code Map}。响应则是对应的镜像：每个
 * 模型都有一个静态的 {@code fromJson(Map)}。
 */
public interface JsonBody {

    /** 作为请求负载发送的 JSON 对象。 */
    Map<String, Object> toJson();
}

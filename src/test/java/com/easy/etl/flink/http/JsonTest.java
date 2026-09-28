package com.easy.etl.flink.http;

import com.easy.etl.flink.http.util.Json;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JsonTest {

    @Test
    public void parseObjectWithNesting() {
        String json = "{\"sessionHandle\":\"abc-123\",\"nested\":{\"list\":[1,2.5,\"x\",true,null]}}";
        Map<String, Object> m = Json.parseObject(json);
        assertEquals("abc-123", m.get("sessionHandle"));
        Map<String, Object> nested = (Map<String, Object>) m.get("nested");
        List<Object> list = (List<Object>) nested.get("list");
        assertEquals(Long.valueOf(1L), list.get(0));
        assertEquals(new BigDecimal("2.5"), list.get(1));
        assertEquals("x", list.get(2));
        assertEquals(Boolean.TRUE, list.get(3));
        assertNull(list.get(4));
    }

    @Test
    public void integralNumbersPreservePrecision() {
        // 2^53 + 1：如果用 double 存储会被舍入为 9007199254740992。
        Map<String, Object> m = Json.parseObject("{\"v\":9007199254740993}");
        assertEquals(Long.valueOf(9007199254740993L), m.get("v"));

        // 超出 long 范围：通过 BigDecimal 保留每一位数字。
        String huge = "12345678901234567890123456789012345678";
        Map<String, Object> big = Json.parseObject("{\"v\":" + huge + "}");
        assertEquals(new BigDecimal(huge), big.get("v"));
    }

    @Test
    public void writeEscapesJsonSpecialCharacters() {
        String raw = "quote\" backslash\\ tab\t newline\n end";
        Map<String, Object> m = new java.util.LinkedHashMap<String, Object>();
        m.put("v", raw);
        String json = Json.write(m);
        // 序列化形式中这些字面字符必须被转义。
        assertTrue(json.contains("\\\""));
        assertTrue(json.contains("\\\\"));
        assertTrue(json.contains("\\n"));
        // 往返序列化能精确还原原始值。
        assertEquals(raw, Json.parseObject(json).get("v"));
    }

    @Test
    public void parseGatewayResultPayload() {
        String json = "{"
                + "\"resultType\":\"PAYLOAD\","
                + "\"resultKind\":\"SUCCESS_WITH_CONTENT\","
                + "\"nextResultUri\":\"/v1/sessions/s/operations/o/result/1\","
                + "\"results\":{"
                + "  \"columns\":[{\"name\":\"a\"},{\"name\":\"b\"}],"
                + "  \"data\":[[\"1\",\"hello\"],[\"2\",\"world\"]]"
                + "}}";
        Map<String, Object> m = Json.parseObject(json);
        assertEquals("PAYLOAD", m.get("resultType"));
        assertEquals("SUCCESS_WITH_CONTENT", m.get("resultKind"));
        Map<String, Object> results = (Map<String, Object>) m.get("results");
        List<Object> data = (List<Object>) results.get("data");
        assertEquals(2, data.size());
    }

    @Test
    public void writeAndEscape() {
        Map<String, Object> m = new java.util.LinkedHashMap<String, Object>();
        m.put("statement", "SELECT 'it''s'");
        m.put("limit", 10);
        String json = Json.write(m);
        assertTrue(json.contains("SELECT 'it''s'"));
        assertTrue(json.contains("10"));
        // round trip
        Map<String, Object> back = Json.parseObject(json);
        assertEquals("SELECT 'it''s'", back.get("statement"));
    }

    @Test
    public void parseEmptyObjectAndArray() {
        assertEquals(0, Json.parseObject("{}").size());
        assertEquals(0, Json.parseArray("[]").size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectInvalidJson() {
        Json.parse("{not json");
    }
}

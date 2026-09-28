package com.easy.etl.flink.http;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GatewayConfigTest {

    @Test
    public void acceptsHttpAndHttpsAddresses() {
        assertTrue(GatewayConfig.acceptsAddress("http://localhost:8083"));
        assertTrue(GatewayConfig.acceptsAddress("https://gw.example.com:8443?a=b"));
        assertFalse(GatewayConfig.acceptsAddress("localhost:8083"));
        assertFalse(GatewayConfig.acceptsAddress("jdbc:mysql://localhost:3306/db"));
        assertFalse(GatewayConfig.acceptsAddress("jdbc:flink:localhost:8083"));
        assertFalse(GatewayConfig.acceptsAddress(null));
    }

    @Test
    public void acceptsOptionalJdbcPrefix() {
        assertTrue(GatewayConfig.acceptsAddress("jdbc:flink:http://localhost:8083"));
        assertTrue(GatewayConfig.acceptsAddress("JDBC:FLINK:https://gw:8443?user=a"));
    }

    @Test
    public void parsesJdbcPrefixedAddress() {
        GatewayConfig c = GatewayConfig.parse(
                "jdbc:flink:http://gw:8083?user=admin&password=p%40ss&rowFormat=json");
        assertEquals("http://gw:8083", c.getEndpoint());
        assertEquals("http://gw:8083/v1", c.getBaseUrl());
        assertEquals("admin", c.getUser());
        assertEquals("p@ss", c.getPassword());
        assertEquals("json", c.getRowFormat());
    }

    @Test
    public void parsesCredentialsAndRowFormat() {
        GatewayConfig c = GatewayConfig.parse(
                "http://gw:8083?user=admin&password=p%40ss&rowFormat=json");
        assertEquals("http://gw:8083", c.getEndpoint());
        assertEquals("http://gw:8083/v1", c.getBaseUrl());
        assertEquals("admin", c.getUser());
        assertEquals("p@ss", c.getPassword());
        assertEquals("json", c.getRowFormat());
    }

    @Test
    public void stripsTrailingSlashesAndVersionPath() {
        assertEquals("https://gw.example.com:8443/v1",
                GatewayConfig.parse("https://gw.example.com:8443/v1/").getBaseUrl());
        assertEquals("http://localhost:8083/v1",
                GatewayConfig.parse("http://localhost:8083/v1/").getBaseUrl());
    }

    @Test
    public void parsesSessionPropertiesAndTimeouts() {
        GatewayConfig c = GatewayConfig.parse("http://gw:8083"
                + "?user=admin&password=p%40ss&pollIntervalMs=100&maxRows=50"
                + "&operationTimeoutMs=9000"
                + "&sessionProperty.execution.runtime-mode=batch");
        assertEquals(100, c.getPollIntervalMs());
        assertEquals(50, c.getMaxRows());
        assertEquals(9000L, c.getOperationTimeoutMs());
        assertEquals("batch", c.getSessionProperties().get("execution.runtime-mode"));

        String header = c.getBasicAuthHeader();
        assertTrue(header.startsWith("Basic "));
        String decoded = new String(
                java.util.Base64.getDecoder().decode(header.substring("Basic ".length())),
                java.nio.charset.StandardCharsets.UTF_8);
        assertEquals("admin:p@ss", decoded);
    }

    @Test
    public void noCredentialsMeansNoAuthHeader() {
        GatewayConfig c = GatewayConfig.parse("http://localhost:8083");
        assertNull(c.getUser());
        assertFalse(c.hasCredentials());
        assertNull(c.getBasicAuthHeader());
    }

    @Test
    public void overridesWinOverAddressParameters() {
        Properties overrides = new Properties();
        overrides.setProperty("user", "override");
        GatewayConfig c = GatewayConfig.parse("http://gw:8083?user=fromUrl", overrides);
        assertEquals("override", c.getUser());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsNonHttpAddress() {
        GatewayConfig.parse("localhost:8083");
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsUnknownRowFormat() {
        GatewayConfig.parse("http://gw:8083?rowFormat=xml");
    }
}

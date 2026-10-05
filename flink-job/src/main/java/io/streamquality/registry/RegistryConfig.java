package io.streamquality.registry;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Properties;

/** Parsed registry.properties (sq.registry.*). Serializable so it can ship inside operators. */
public record RegistryConfig(boolean enabled, String type, String url, String group, String artifactIdTemplate,
                             String authType, String username, String password, String bearerToken,
                             long refreshMs, int timeoutMs, int breakerFailures, long breakerResetMs) implements Serializable {

    public static RegistryConfig from(Properties p) {
        return new RegistryConfig(
                Boolean.parseBoolean(p.getProperty("sq.registry.enabled", "false")),
                p.getProperty("sq.registry.type", "apicurio"),
                strip(p.getProperty("sq.registry.url", "")),
                p.getProperty("sq.registry.group", "default"),
                p.getProperty("sq.registry.artifact.id.template", "{topic}-value"),
                p.getProperty("sq.registry.auth.type", "none"),
                p.getProperty("sq.registry.auth.username", ""),
                p.getProperty("sq.registry.auth.password", ""),
                p.getProperty("sq.registry.auth.token", ""),
                Long.parseLong(p.getProperty("sq.registry.refresh.interval.ms", "300000")),
                Integer.parseInt(p.getProperty("sq.registry.timeout.ms", "3000")),
                Integer.parseInt(p.getProperty("sq.registry.breaker.failures", "3")),
                Long.parseLong(p.getProperty("sq.registry.breaker.reset.ms", "60000")));
    }

    public String artifactId(String topic) { return artifactIdTemplate.replace("{topic}", topic); }

    private static String strip(String u) { return u.endsWith("/") ? u.substring(0, u.length() - 1) : u; }
}

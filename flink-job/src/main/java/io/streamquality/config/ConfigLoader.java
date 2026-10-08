package io.streamquality.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

/**
 * Loads *.properties from --config-dir. Resolves ${env:NAME} and ${env:NAME:-default}. Splits keys:
 *   plain keys   -> handed verbatim to Kafka clients
 *   "sq."-prefixed -> monitor settings
 * Nothing about the Kafka flavour is interpreted here.
 */
public final class ConfigLoader {
    private static final Pattern ENV = Pattern.compile("\\$\\{env:([A-Za-z0-9_]+)(?::-([^}]*))?}");

    private ConfigLoader() {}

    public static Properties load(Path file) throws IOException { return load(file, System::getenv); }

    /** env: variable lookup (null = unset). Lets tests resolve ${env:...} from a file instead of the process environment. */
    public static Properties load(Path file, UnaryOperator<String> env) throws IOException {
        Properties raw = new Properties();
        try (InputStream in = new FileInputStream(file.toFile())) { raw.load(in); }
        Properties out = new Properties();
        for (String k : raw.stringPropertyNames()) out.setProperty(k, resolveEnv(raw.getProperty(k), env));
        return out;
    }

    static String resolveEnv(String v, UnaryOperator<String> env) {
        Matcher m = ENV.matcher(v);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String val = env.apply(m.group(1));
            if (val == null) val = m.group(2);                      // ${env:NAME:-default}
            if (val == null) throw new IllegalStateException("Missing env var " + m.group(1) + " referenced in config");
            m.appendReplacement(sb, Matcher.quoteReplacement(val));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /** Properties safe to give to KafkaSource/KafkaSink: drop sq.* keys. */
    public static Properties kafkaClientProps(Properties all) {
        Properties p = new Properties();
        for (String k : all.stringPropertyNames()) if (!k.startsWith("sq.")) p.setProperty(k, all.getProperty(k));
        return p;
    }

    public static Map<String, String> sqProps(Properties all) {
        Map<String, String> m = new HashMap<>();
        for (String k : all.stringPropertyNames()) if (k.startsWith("sq.")) m.put(k, all.getProperty(k));
        return m;
    }

    public record SqConfig(Path dir, Properties kafka, Properties registry) implements Serializable {}
}

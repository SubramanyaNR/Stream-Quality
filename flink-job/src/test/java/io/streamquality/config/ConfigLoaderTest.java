package io.streamquality.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigLoaderTest {
    @TempDir Path tmp;

    @Test
    void resolvesEnvAndSplitsSqKeys() throws Exception {
        Path f = tmp.resolve("k.properties");
        Files.writeString(f, "bootstrap.servers=${env:PATH}\nsecurity.protocol=PLAINTEXT\nsq.dlq.topic=dlq\n");
        Properties p = ConfigLoader.load(f);
        assertThat(p.getProperty("bootstrap.servers")).isEqualTo(System.getenv("PATH"));
        Properties client = ConfigLoader.kafkaClientProps(p);
        assertThat(client.stringPropertyNames()).containsExactlyInAnyOrder("bootstrap.servers", "security.protocol");
        assertThat(ConfigLoader.sqProps(p)).containsEntry("sq.dlq.topic", "dlq").hasSize(1);
    }

    @Test
    void missingEnvVarFailsFast() throws Exception {
        Path f = tmp.resolve("k.properties");
        Files.writeString(f, "sasl.jaas.config=${env:SQ_DEFINITELY_NOT_SET_123}\n");
        assertThatThrownBy(() -> ConfigLoader.load(f)).hasMessageContaining("SQ_DEFINITELY_NOT_SET_123");
    }

    @Test
    void defaultsApplyOnlyWhenTheVariableIsUnset() throws Exception {
        Path f = tmp.resolve("k.properties");
        Files.writeString(f, "a=${env:SQ_NOT_SET_XYZ:-apicurio}\nb=${env:PATH:-ignored}\nc=${env:SQ_NOT_SET_XYZ:-}\nd=x-${env:SQ_NOT_SET_XYZ:-1}-${env:SQ_NOT_SET_XYZ:-2}\n");
        Properties p = ConfigLoader.load(f);
        assertThat(p.getProperty("a")).isEqualTo("apicurio");
        assertThat(p.getProperty("b")).isEqualTo(System.getenv("PATH"));
        assertThat(p.getProperty("c")).isEmpty();                         // an explicitly empty default is a valid value
        assertThat(p.getProperty("d")).isEqualTo("x-1-2");
    }

    /** java.util.Properties has NO inline comments: `key=value   # note` makes the note part of the value. */
    @Test
    void shippedPropertiesFilesHaveNoInlineComments() throws Exception {
        Path cfg = Path.of("..", "config").toAbsolutePath().normalize();
        try (var files = Files.list(cfg)) {
            for (Path f : files.filter(x -> x.toString().endsWith(".properties")).toList()) {
                for (String line : Files.readAllLines(f)) {
                    String t = line.strip();
                    if (t.isEmpty() || t.startsWith("#") || !t.contains("=")) continue;
                    assertThat(t.substring(t.indexOf('=') + 1)).as(f.getFileName() + ": " + t).doesNotContain(" #");
                }
            }
        }
    }

    /** The templates reference ${env:...} variables defined in cluster.env.example, so resolve them from that file: no exported env needed. */
    @Test
    void shippedTemplatesParse() throws Exception {
        Path repo = Path.of("..").toAbsolutePath().normalize();
        var vars = new java.util.HashMap<String, String>();
        for (String line : Files.readAllLines(repo.resolve("cluster.env.example"))) {
            if (line.isBlank() || line.startsWith("#") || !line.contains("=")) continue;
            vars.put(line.substring(0, line.indexOf('=')), line.substring(line.indexOf('=') + 1));
        }
        Properties k = ConfigLoader.load(repo.resolve("config/kafka.properties"), vars::get);
        assertThat(k.getProperty("bootstrap.servers")).isNotBlank();
        Properties r = ConfigLoader.load(repo.resolve("config/registry.properties"), vars::get);
        assertThat(r.getProperty("sq.registry.type")).isEqualTo("apicurio");     // from REGISTRY_TYPE in cluster.env.example
        assertThat(r.getProperty("sq.registry.group")).isEqualTo("default");     // not "default   # note": no inline comments
        // an older cluster.env without REGISTRY_TYPE keeps working: only that variable unset => default
        Properties legacy = ConfigLoader.load(repo.resolve("config/registry.properties"), n -> n.equals("REGISTRY_TYPE") ? null : vars.get(n));
        assertThat(legacy.getProperty("sq.registry.type")).isEqualTo("apicurio");
    }
}

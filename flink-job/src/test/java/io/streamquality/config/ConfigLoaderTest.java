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
    void shippedTemplatesParse() throws Exception {
        Path repoConfig = Path.of("..", "config").toAbsolutePath().normalize();
        Properties k = ConfigLoader.load(repoConfig.resolve("kafka.properties"));
        assertThat(k.getProperty("bootstrap.servers")).isNotBlank();
        assertThat(ConfigLoader.load(repoConfig.resolve("registry.properties")).getProperty("sq.registry.type")).isEqualTo("apicurio");
    }
}

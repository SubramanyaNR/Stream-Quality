package io.streamquality.registry;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class SchemaProviderTest {
    static final String SCHEMA = """
            {"$schema":"http://json-schema.org/draft-07/schema#","type":"object",
             "required":["id"],"properties":{"id":{"type":"string"},"amount":{"type":"number"}}}""";
    static final ObjectMapper M = new ObjectMapper();

    static RegistryConfig cfg(long refreshMs) {
        var p = new java.util.Properties();
        p.setProperty("sq.registry.enabled", "true");
        p.setProperty("sq.registry.refresh.interval.ms", String.valueOf(refreshMs));
        p.setProperty("sq.registry.breaker.failures", "2");
        p.setProperty("sq.registry.breaker.reset.ms", "10000");
        return RegistryConfig.from(p);
    }

    /** Scriptable fake registry. */
    static class Fake implements RegistryClient {
        volatile boolean down; volatile boolean none; final AtomicInteger calls = new AtomicInteger();
        public Optional<String> fetchLatestSchema(String id) throws IOException {
            calls.incrementAndGet();
            if (down) throw new IOException("connection refused");
            return none ? Optional.empty() : Optional.of(SCHEMA);
        }
        public boolean ping() { return !down; }
    }

    @Test void validatesAgainstRegistrySchema() throws Exception {
        var p = new SchemaProvider(cfg(60_000), new Fake(), () -> 0);
        assertThat(p.validate("t", M.readTree("{\"id\":\"a\",\"amount\":1.5}")).state()).isEqualTo(SchemaProvider.State.VALID);
        var bad = p.validate("t", M.readTree("{\"id\":\"a\",\"amount\":\"N/A\"}"));
        assertThat(bad.state()).isEqualTo(SchemaProvider.State.INVALID);
        assertThat(bad.error()).contains("amount");
        assertThat(p.validate("t", M.readTree("{\"amount\":1}")).error()).contains("id");   // required missing
        p.close();
    }

    @Test void registryDownFromTheStartSkipsInsteadOfFailing() throws Exception {
        var f = new Fake(); f.down = true;
        var p = new SchemaProvider(cfg(60_000), f, () -> 0);
        for (int i = 0; i < 10; i++)
            assertThat(p.validate("t", M.readTree("{\"id\":1}")).state()).isEqualTo(SchemaProvider.State.SKIPPED);
        assertThat(p.isUp()).isFalse();
        assertThat(f.calls.get()).isEqualTo(2);        // breaker (threshold 2) stopped hammering the dead registry
        p.close();
    }

    @Test void keepsLastGoodSchemaWhenRegistryDiesAndRecovers() throws Exception {
        var now = new AtomicLong(0);
        var f = new Fake();
        var p = new SchemaProvider(cfg(1000), f, now::get);
        assertThat(p.validate("t", M.readTree("{\"id\":\"a\"}")).state()).isEqualTo(SchemaProvider.State.VALID);
        f.down = true; now.set(5000);                  // stale -> background refresh will fail
        for (int i = 0; i < 3; i++) {
            assertThat(p.validate("t", M.readTree("{\"id\":\"a\"}")).state()).isEqualTo(SchemaProvider.State.VALID);   // last good still used
            Thread.sleep(100);
        }
        assertThat(p.validate("t", M.readTree("{\"id\":5}")).state()).isEqualTo(SchemaProvider.State.INVALID);       // and still enforced
        f.down = false; now.set(30_000);               // breaker reset window passed
        p.validate("t", M.readTree("{\"id\":\"a\"}")); Thread.sleep(300);
        assertThat(p.isUp()).isTrue();
        p.close();
    }

    @Test void topicWithoutSchemaIsSkippedNotInvalid() throws Exception {
        var f = new Fake(); f.none = true;
        var p = new SchemaProvider(cfg(60_000), f, () -> 0);
        assertThat(p.validate("t", M.readTree("{}")).state()).isEqualTo(SchemaProvider.State.SKIPPED);
        assertThat(p.isUp()).isTrue();                 // 404 is a healthy registry
        p.close();
    }

    @Test void newlyRegisteredSchemaIsPickedUpQuicklyNotAfterTheFullRefreshInterval() throws Exception {
        var now = new AtomicLong(0);
        var f = new Fake(); f.none = true;
        var p = new SchemaProvider(cfg(300_000), f, now::get);          // 5 min refresh
        assertThat(p.validate("t", M.readTree("{\"id\":5}")).state()).isEqualTo(SchemaProvider.State.SKIPPED);
        f.none = false;                                                  // schema gets registered
        now.set(31_000);                                                 // just past the 30 s negative TTL
        p.validate("t", M.readTree("{\"id\":5}")); Thread.sleep(300);   // triggers background refresh
        assertThat(p.validate("t", M.readTree("{\"id\":5}")).state()).isEqualTo(SchemaProvider.State.INVALID);
        p.close();
    }

    @Test void honoursDeclaredSpecVersion() throws Exception {
        assertThat(SchemaProvider.specOf(M.readTree("{\"$schema\":\"http://json-schema.org/draft-04/schema#\"}")).name()).isEqualTo("V4");
        assertThat(SchemaProvider.specOf(M.readTree("{}")).name()).isEqualTo("V202012");
    }
}

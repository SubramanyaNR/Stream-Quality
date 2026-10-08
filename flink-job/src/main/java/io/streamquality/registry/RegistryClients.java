package io.streamquality.registry;

import java.util.Set;

/** Maps sq.registry.type to a client. The Confluent REST API is spoken by several registries, hence the aliases. */
public final class RegistryClients {
    private RegistryClients() {}

    public static final Set<String> CONFLUENT_API = Set.of("confluent", "karapace", "redpanda", "ccompat");

    public static RegistryClient create(RegistryConfig cfg) {
        String t = cfg.type().toLowerCase();
        if (t.equals("apicurio")) return new ApicurioV3Client(cfg);
        if (CONFLUENT_API.contains(t)) return new ConfluentRegistryClient(cfg);
        throw new IllegalStateException("Unsupported sq.registry.type '" + cfg.type()
                + "' (supported: apicurio, confluent | karapace | redpanda | ccompat)");
    }
}

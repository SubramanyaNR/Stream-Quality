package io.streamquality.registry;

import java.io.IOException;
import java.util.Optional;

/** Vendor-neutral contract. Adding Confluent later = another implementation selected by sq.registry.type. */
public interface RegistryClient {
    /** Latest JSON Schema text for the artifact; empty if the registry has none (404). Throws if unreachable/erroring. */
    Optional<String> fetchLatestSchema(String artifactId) throws IOException;

    /** Cheap liveness probe. */
    boolean ping();
}

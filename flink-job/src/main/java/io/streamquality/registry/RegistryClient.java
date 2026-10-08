package io.streamquality.registry;

import java.io.IOException;
import java.util.Optional;

/** Vendor-neutral contract; implementations are selected by sq.registry.type (see RegistryClients). */
public interface RegistryClient {
    /** Latest schema for a subject/artifact; empty if the registry has none (404). Throws if unreachable/erroring. */
    Optional<RegisteredSchema> fetchLatest(String subject) throws IOException;

    /** Whether schemas can be looked up by the 4-byte id found in the Confluent wire format. */
    default boolean supportsIds() { return false; }

    /** Schema by registry-wide id (wire-format header); empty if unknown (404). */
    default Optional<RegisteredSchema> fetchById(int id) throws IOException {
        throw new UnsupportedOperationException("this registry type does not support lookup by schema id");
    }

    /** Cheap liveness probe. */
    boolean ping();
}

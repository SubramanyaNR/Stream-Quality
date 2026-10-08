package io.streamquality.decode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

/**
 * Avro datum -> plain Jackson tree, so every check sees the same shape regardless of wire format.
 * Unions are transparent (a nullable string is just a string or null), bytes/fixed become base64 text,
 * enums and Utf8 become strings. Logical types stay in their underlying representation (timestamp-millis = long).
 */
final class AvroJson {
    private AvroJson() {}

    static JsonNode toJson(Object v, ObjectMapper m) {
        if (v == null) return m.nullNode();
        if (v instanceof GenericRecord r) {
            ObjectNode o = m.createObjectNode();
            for (var f : r.getSchema().getFields()) o.set(f.name(), toJson(r.get(f.pos()), m));
            return o;
        }
        if (v instanceof CharSequence cs) return m.getNodeFactory().textNode(cs.toString());
        if (v instanceof Boolean b) return m.getNodeFactory().booleanNode(b);
        if (v instanceof Integer i) return m.getNodeFactory().numberNode(i);
        if (v instanceof Long l) return m.getNodeFactory().numberNode(l);
        if (v instanceof Float f) return m.getNodeFactory().numberNode(f);
        if (v instanceof Double d) return m.getNodeFactory().numberNode(d);
        if (v instanceof ByteBuffer bb) {
            byte[] b = new byte[bb.remaining()];
            bb.duplicate().get(b);
            return m.getNodeFactory().textNode(Base64.getEncoder().encodeToString(b));
        }
        if (v instanceof GenericFixed fx) return m.getNodeFactory().textNode(Base64.getEncoder().encodeToString(fx.bytes()));
        if (v instanceof Map<?, ?> map) {
            ObjectNode o = m.createObjectNode();
            map.forEach((k, val) -> o.set(String.valueOf(k), toJson(val, m)));
            return o;
        }
        if (v instanceof Collection<?> c) {
            ArrayNode a = m.createArrayNode();
            c.forEach(x -> a.add(toJson(x, m)));
            return a;
        }
        return m.getNodeFactory().textNode(v.toString());       // enum symbols and anything else
    }
}

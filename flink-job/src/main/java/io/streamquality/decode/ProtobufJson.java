package io.streamquality.decode;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.squareup.wire.Syntax;
import com.squareup.wire.schema.EnumType;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.MessageType;
import com.squareup.wire.schema.ProtoFile;
import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.Schema;
import com.squareup.wire.schema.SchemaLoader;
import com.squareup.wire.schema.Type;
import io.streamquality.registry.RegisteredSchema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import okio.ByteString;

/**
 * Decodes Confluent-framed Protobuf: after the 5-byte header comes the message-index list (which message of the .proto
 * the payload is: zigzag varints, [0] shortened to a single 0 byte), then the protobuf bytes. The writer schema (.proto
 * text, plus the schemas it imports via registry references) is parsed with Wire's schema loader; messages are decoded
 * dynamically, no generated classes, and converted to the same JSON tree the other formats produce.
 * proto3 fields without presence (plain scalars/enums) are not serialized when they hold the default; they are filled in
 * so that a legitimate 0 / "" / false is not mistaken for a missing value by the null-rate check. `optional` fields,
 * oneof members, messages, repeated and map fields stay absent when unset.
 */
public final class ProtobufJson {
    /** Resolves a registry reference (import) to its schema text; returns null when it does not exist. */
    @FunctionalInterface public interface ReferenceResolver { RegisteredSchema resolve(RegisteredSchema.Reference ref) throws IOException; }

    /** The schema or payload is bad (as opposed to the registry being unreachable, which surfaces as a plain IOException). */
    public static final class DecodeException extends IOException {
        public DecodeException(String m) { super(m); }
        public DecodeException(String m, Throwable c) { super(m, c); }
    }

    private record Compiled(Schema schema, ProtoFile file) {}

    private final ObjectMapper mapper;
    private final ReferenceResolver resolver;
    private final Map<Integer, Compiled> compiled = new ConcurrentHashMap<>();

    public ProtobufJson(ObjectMapper mapper, ReferenceResolver resolver) { this.mapper = mapper; this.resolver = resolver; }

    /** @param value the whole Kafka value; body starts at `offset` (right after the 5-byte header) */
    public JsonNode decode(int schemaId, RegisteredSchema schema, byte[] value, int offset) throws IOException {
        Compiled c = compiled.get(schemaId);
        if (c == null) { c = compile(schema); compiled.put(schemaId, c); }
        int[] pos = {offset};
        int count = (int) zigzag(value, pos);
        int[] path = count == 0 ? new int[] {0} : new int[count];
        for (int i = 0; count != 0 && i < count; i++) path[i] = (int) zigzag(value, pos);
        MessageType msg = locate(c.file, path);
        String name = msg.getType().toString();
        try { return message(value, pos[0], value.length, msg, c.schema, 0); }
        catch (DecodeException e) { throw e; }
        catch (Exception e) { throw new DecodeException("malformed protobuf body: " + e.getMessage(), e); }
    }

    // ---------------------------------------------------------------- schema compilation
    private Compiled compile(RegisteredSchema s) throws IOException {
        Path dir = Files.createTempDirectory("sq-proto");
        try {
            write(dir, "schema.proto", s.text());
            writeRefs(dir, s.references(), 0);
            SchemaLoader loader = new SchemaLoader(dir.getFileSystem());
            loader.initRoots(List.of(Location.get(dir.toString())), List.of());
            Schema schema = loader.loadSchema();
            ProtoFile f = schema.protoFile("schema.proto");
            if (f == null) throw new DecodeException("schema.proto not loaded");
            return new Compiled(schema, f);
        } catch (DecodeException e) { throw e;
        } catch (IOException e) { throw e;                        // registry unreachable while resolving a reference: caller degrades
        } catch (Exception e) { throw new DecodeException("cannot compile .proto: " + e.getMessage(), e);
        } finally { delete(dir); }
    }

    private void writeRefs(Path dir, List<RegisteredSchema.Reference> refs, int depth) throws IOException {
        if (refs == null || depth > 8) return;
        for (RegisteredSchema.Reference r : refs) {
            RegisteredSchema rs = resolver.resolve(r);
            if (rs == null) throw new DecodeException("referenced schema " + r.subject() + " v" + r.version() + " not found");
            write(dir, r.name(), rs.text());
            writeRefs(dir, rs.references(), depth + 1);
        }
    }

    private static void write(Path dir, String name, String text) throws IOException {
        Path p = dir.resolve(name).normalize();
        if (!p.startsWith(dir)) throw new DecodeException("illegal reference name " + name);   // never write outside the temp dir
        if (p.getParent() != null) Files.createDirectories(p.getParent());
        Files.writeString(p, text, StandardCharsets.UTF_8);
    }

    private static void delete(Path dir) {
        try {
            Files.walkFileTree(dir, new SimpleFileVisitor<>() {
                @Override public FileVisitResult visitFile(Path f, BasicFileAttributes a) throws IOException { Files.delete(f); return FileVisitResult.CONTINUE; }
                @Override public FileVisitResult postVisitDirectory(Path d, IOException e) throws IOException { Files.delete(d); return FileVisitResult.CONTINUE; }
            });
        } catch (IOException ignored) { /* temp dir: best effort */ }
    }

    private static MessageType locate(ProtoFile f, int[] path) throws IOException {
        List<Type> level = f.getTypes();
        MessageType found = null;
        for (int idx : path) {
            // message indexes count only messages (enums share the declaration list in Wire, so skip them)
            int n = -1; Type hit = null;
            for (Type t : level) if (t instanceof MessageType) { if (++n == idx) { hit = t; break; } }
            if (hit == null) throw new DecodeException("message index " + idx + " not found in schema");
            found = (MessageType) hit;
            level = hit.getNestedTypes();
        }
        return found;
    }

    private static long zigzag(byte[] b, int[] pos) throws IOException {
        long v = 0; int shift = 0;
        while (true) {
            if (pos[0] >= b.length || shift > 63) throw new DecodeException("truncated message-index header");
            byte x = b[pos[0]++];
            v |= (long) (x & 0x7F) << shift;
            if ((x & 0x80) == 0) break;
            shift += 7;
        }
        return (v >>> 1) ^ -(v & 1);
    }

    // ---------------------------------------------------------------- protobuf wire format -> JSON (Wire supplies only the schema)
    private static final int MAX_DEPTH = 32;

    private ObjectNode message(byte[] b, int start, int end, MessageType type, Schema schema, int depth) throws IOException {
        if (depth > MAX_DEPTH) throw new DecodeException("message nesting deeper than " + MAX_DEPTH);
        Map<Integer, Field> byTag = new java.util.HashMap<>();
        for (Field f : type.getFieldsAndOneOfFields()) byTag.put(f.getTag(), f);
        ObjectNode out = mapper.createObjectNode();
        int[] p = {start};
        while (p[0] < end) {
            long key = varint(b, p, end);
            int tag = (int) (key >>> 3), wire = (int) (key & 7);
            Field f = byTag.get(tag);
            if (f == null) { skip(b, p, end, wire); continue; }           // unknown field: ignore
            ProtoType ft = f.getType();
            if (ft.isMap()) {
                byte[] entry = lengthDelimited(b, p, end, wire);
                putMapEntry(out, f, entry, schema, depth);
                continue;
            }
            if (f.isRepeated() && wire == 2 && isPackable(ft, schema)) {  // packed scalars
                int len = (int) varint(b, p, end);
                int stop = p[0] + len;
                if (len < 0 || stop > end) throw new DecodeException("truncated packed field " + f.getName());
                ArrayNode arr = array(out, f.getName());
                while (p[0] < stop) arr.add(scalar(b, p, stop, ft, packedWire(ft, schema), schema));
                continue;
            }
            JsonNode v;
            if (wire == 2 && !isScalarLengthType(ft, schema)) {            // nested message
                byte[] sub = lengthDelimited(b, p, end, wire);
                v = message(sub, 0, sub.length, (MessageType) schema.getType(ft), schema, depth + 1);
            } else v = scalar(b, p, end, ft, wire, schema);
            if (f.isRepeated()) array(out, f.getName()).add(v); else out.set(f.getName(), v);
        }
        for (Field f : type.getFieldsAndOneOfFields()) if (!out.has(f.getName()) && needsDefault(f, type, schema)) out.set(f.getName(), defaultFor(f, schema));
        return out;
    }

    private void putMapEntry(ObjectNode out, Field f, byte[] entry, Schema schema, int depth) throws IOException {
        ProtoType kt = f.getType().getKeyType(), vt = f.getType().getValueType();
        ObjectNode obj = out.has(f.getName()) ? (ObjectNode) out.get(f.getName()) : out.putObject(f.getName());
        int[] p = {0};
        String k = ""; JsonNode v = null;
        while (p[0] < entry.length) {
            long key = varint(entry, p, entry.length);
            int tag = (int) (key >>> 3), wire = (int) (key & 7);
            if (tag == 1) k = scalar(entry, p, entry.length, kt, wire, schema).asText();
            else if (tag == 2) {
                Type t = schema.getType(vt);
                if (t instanceof MessageType mt) { byte[] sub = lengthDelimited(entry, p, entry.length, wire); v = message(sub, 0, sub.length, mt, schema, depth + 1); }
                else v = scalar(entry, p, entry.length, vt, wire, schema);
            } else skip(entry, p, entry.length, wire);
        }
        obj.set(k, v == null ? defaultValue(vt, schema) : v);
    }

    private JsonNode scalar(byte[] b, int[] p, int end, ProtoType t, int wire, Schema schema) throws IOException {
        var nf = mapper.getNodeFactory();
        Type ct = schema.getType(t);
        if (ct instanceof EnumType et) {
            int n = (int) varint(b, p, end);
            return nf.textNode(et.getConstants().stream().filter(c -> c.getTag() == n).map(c -> c.getName()).findFirst().orElse(String.valueOf(n)));
        }
        switch (t.toString()) {
            case "string": return nf.textNode(new String(lengthDelimited(b, p, end, wire), StandardCharsets.UTF_8));
            case "bytes": return nf.textNode(Base64.getEncoder().encodeToString(lengthDelimited(b, p, end, wire)));
            case "bool": return nf.booleanNode(varint(b, p, end) != 0);
            case "int32": return nf.numberNode((int) varint(b, p, end));
            case "uint32": return nf.numberNode(varint(b, p, end) & 0xFFFFFFFFL);
            case "int64": return nf.numberNode(varint(b, p, end));
            case "uint64": { long v = varint(b, p, end); return v >= 0 ? nf.numberNode(v) : nf.textNode(Long.toUnsignedString(v)); }
            case "sint32": { long v = varint(b, p, end); return nf.numberNode((int) ((v >>> 1) ^ -(v & 1))); }
            case "sint64": { long v = varint(b, p, end); return nf.numberNode((v >>> 1) ^ -(v & 1)); }
            case "fixed32": return nf.numberNode(fixed(b, p, end, 4) & 0xFFFFFFFFL);
            case "sfixed32": return nf.numberNode((int) fixed(b, p, end, 4));
            case "float": return nf.numberNode(Float.intBitsToFloat((int) fixed(b, p, end, 4)));
            case "fixed64": { long v = fixed(b, p, end, 8); return v >= 0 ? nf.numberNode(v) : nf.textNode(Long.toUnsignedString(v)); }
            case "sfixed64": return nf.numberNode(fixed(b, p, end, 8));
            case "double": return nf.numberNode(Double.longBitsToDouble(fixed(b, p, end, 8)));
            default: throw new DecodeException("unsupported protobuf type " + t);
        }
    }

    private static boolean isScalarLengthType(ProtoType t, Schema s) {
        String n = t.toString();
        return n.equals("string") || n.equals("bytes");
    }

    private static boolean isPackable(ProtoType t, Schema s) {
        return !(isScalarLengthType(t, s) || s.getType(t) instanceof MessageType);
    }

    private static int packedWire(ProtoType t, Schema s) {
        return switch (t.toString()) { case "fixed32", "sfixed32", "float" -> 5; case "fixed64", "sfixed64", "double" -> 1; default -> 0; };
    }

    private ArrayNode array(ObjectNode o, String name) { return o.has(name) ? (ArrayNode) o.get(name) : o.putArray(name); }

    private static long varint(byte[] b, int[] p, int end) throws IOException {
        long v = 0; int shift = 0;
        while (true) {
            if (p[0] >= end || shift > 63) throw new DecodeException("truncated varint");
            byte x = b[p[0]++];
            v |= (long) (x & 0x7F) << shift;
            if ((x & 0x80) == 0) return v;
            shift += 7;
        }
    }

    private static long fixed(byte[] b, int[] p, int end, int n) throws IOException {
        if (p[0] + n > end) throw new DecodeException("truncated fixed-width field");
        long v = 0;
        for (int i = 0; i < n; i++) v |= (long) (b[p[0] + i] & 0xFF) << (8 * i);
        p[0] += n;
        return v;
    }

    private static byte[] lengthDelimited(byte[] b, int[] p, int end, int wire) throws IOException {
        if (wire != 2) throw new DecodeException("wire type " + wire + " where a length-delimited field was expected");
        long len = varint(b, p, end);
        if (len < 0 || p[0] + len > end) throw new DecodeException("length " + len + " exceeds the remaining " + (end - p[0]) + " bytes");
        byte[] r = Arrays.copyOfRange(b, p[0], p[0] + (int) len);
        p[0] += (int) len;
        return r;
    }

    private static void skip(byte[] b, int[] p, int end, int wire) throws IOException {
        switch (wire) {
            case 0: varint(b, p, end); break;
            case 1: fixed(b, p, end, 8); break;
            case 2: lengthDelimited(b, p, end, 2); break;
            case 5: fixed(b, p, end, 4); break;
            default: throw new DecodeException("unsupported wire type " + wire);
        }
    }

    private static boolean needsDefault(Field f, MessageType type, Schema schema) {
        if (type.getSyntax() != Syntax.PROTO_3) return false;
        if (f.isRepeated() || f.isOneOf() || f.getLabel() == Field.Label.OPTIONAL || f.getLabel() == Field.Label.REQUIRED) return false;
        ProtoType pt = f.getType();
        if (pt == null || pt.isMap()) return false;
        Type t = schema.getType(pt);
        return !(t instanceof MessageType);
    }

    private JsonNode defaultFor(Field f, Schema schema) { return defaultValue(f.getType(), schema); }

    private JsonNode defaultValue(ProtoType pt, Schema schema) {
        var nf = mapper.getNodeFactory();
        Type t = schema.getType(pt);
        if (t instanceof MessageType) return nf.objectNode();
        if (t instanceof EnumType et) return nf.textNode(et.getConstants().isEmpty() ? "" : et.getConstants().get(0).getName());
        String n = pt.toString();
        if (n.equals("string")) return nf.textNode("");
        if (n.equals("bool")) return nf.booleanNode(false);
        if (n.equals("bytes")) return nf.textNode("");
        if (n.equals("double") || n.equals("float")) return nf.numberNode(0.0);
        return nf.numberNode(0);
    }
}

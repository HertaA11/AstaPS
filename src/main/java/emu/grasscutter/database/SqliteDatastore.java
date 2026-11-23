package emu.grasscutter.database;

import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.mongodb.MongoClientSettings;
import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Id;
import dev.morphia.annotations.Transient;
import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

/**
 * A single SQLite file holding every document as JSON.
 *
 * <p>Each row is {@code (db, collection, id, doc)}. The JSON is the whole entity, so the file can
 * be opened with any SQLite client and edited by hand: {@code SELECT doc FROM documents WHERE
 * collection='players'} returns readable JSON, and {@code UPDATE documents SET doc=...} works the
 * same way as editing a save file.
 *
 * <p>The API deliberately mirrors the small part of Morphia's Datastore/Query surface that
 * DatabaseHelper uses, so switching storage does not mean rewriting every query.
 */
public final class SqliteDatastore implements AutoCloseable {
    private static final String SCHEMA =
            """
            CREATE TABLE IF NOT EXISTS documents (
                db         TEXT NOT NULL,
                collection TEXT NOT NULL,
                id         TEXT NOT NULL,
                doc        TEXT NOT NULL,
                PRIMARY KEY (db, collection, id)
            )
            """;
    private static final String INDEX =
            "CREATE INDEX IF NOT EXISTS documents_lookup ON documents (db, collection)";
    private static final String COUNTER_SCHEMA =
            """
            CREATE TABLE IF NOT EXISTS counters (
                db    TEXT NOT NULL,
                name  TEXT NOT NULL,
                value INTEGER NOT NULL,
                PRIMARY KEY (db, name)
            )
            """;

    private static final Gson GSON =
            new GsonBuilder()
                    .addSerializationExclusionStrategy(new FieldPolicy())
                    .addDeserializationExclusionStrategy(new FieldPolicy())
                    .registerTypeAdapter(
                            ObjectId.class,
                            new TypeAdapter<ObjectId>() {
                                @Override
                                public void write(JsonWriter out, ObjectId value) throws java.io.IOException {
                                    if (value == null) out.nullValue();
                                    else out.value(value.toHexString());
                                }

                                @Override
                                public ObjectId read(JsonReader in) throws java.io.IOException {
                                    if (in.peek() == JsonToken.NULL) {
                                        in.nextNull();
                                        return null;
                                    }
                                    return new ObjectId(in.nextString());
                                }
                            })
                    .disableHtmlEscaping()
                    .create();

    /** Skips the fields Morphia would not persist either. */
    private static final class FieldPolicy implements ExclusionStrategy {
        @Override
        public boolean shouldSkipField(FieldAttributes attributes) {
            if (attributes.hasModifier(Modifier.STATIC) || attributes.hasModifier(Modifier.TRANSIENT)) {
                return true;
            }
            return attributes.getAnnotation(Transient.class) != null;
        }

        @Override
        public boolean shouldSkipClass(Class<?> clazz) {
            return false;
        }
    }

    private final String dbName;
    private final String path;
    private final Connection connection;

    public SqliteDatastore(String dbName, String path) {
        this.dbName = dbName;
        this.path = path;
        try {
            Class.forName("org.sqlite.JDBC");
            this.connection = DriverManager.getConnection("jdbc:sqlite:" + path);
            try (Statement statement = connection.createStatement()) {
                statement.execute("PRAGMA journal_mode=WAL");
                statement.execute("PRAGMA synchronous=NORMAL");
                statement.execute("PRAGMA busy_timeout=10000");
                statement.execute(SCHEMA);
                statement.execute(INDEX);
                statement.execute(COUNTER_SCHEMA);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not open SQLite database at " + path, e);
        }
    }

    public String getPath() {
        return path;
    }

    // ------------------------------------------------------------------ writes

    public synchronized void save(Object entity) {
        if (entity == null) return;
        String collection = collectionOf(entity.getClass());
        if (collection == null) {
            throw new IllegalArgumentException("Not a stored entity: " + entity.getClass());
        }
        String id = idOf(entity);
        String json = GSON.toJson(entity);
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "INSERT INTO documents (db, collection, id, doc) VALUES (?, ?, ?, ?)"
                                + " ON CONFLICT(db, collection, id) DO UPDATE SET doc=excluded.doc")) {
            statement.setString(1, dbName);
            statement.setString(2, collection);
            statement.setString(3, id);
            statement.setString(4, json);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite save failed for " + collection + "/" + id, e);
        }
    }

    public synchronized void insert(Object entity) {
        if (entity == null) return;
        String collection = collectionOf(entity.getClass());
        if (collection == null) {
            throw new IllegalArgumentException("Not a stored entity: " + entity.getClass());
        }
        String id = idOf(entity);
        String json = GSON.toJson(entity);
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "INSERT INTO documents (db, collection, id, doc) VALUES (?, ?, ?, ?)")) {
            statement.setString(1, dbName);
            statement.setString(2, collection);
            statement.setString(3, id);
            statement.setString(4, json);
            statement.executeUpdate();
        } catch (SQLException e) {
            // Match the driver behaviour callers already handle: a duplicate key is a write error.
            if (e.getMessage() != null && e.getMessage().contains("UNIQUE")) {
                throw new IllegalStateException("Duplicate key " + collection + "/" + id, e);
            }
            throw new IllegalStateException("SQLite insert failed for " + collection + "/" + id, e);
        }
    }

    public synchronized void delete(Object entity) {
        if (entity == null) return;
        String collection = collectionOf(entity.getClass());
        if (collection == null) return;
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "DELETE FROM documents WHERE db=? AND collection=? AND id=?")) {
            statement.setString(1, dbName);
            statement.setString(2, collection);
            statement.setString(3, idOf(entity));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite delete failed for " + collection, e);
        }
    }

    public synchronized void deleteById(Class<?> type, Object id) {
        String collection = collectionOf(type);
        if (collection == null) return;
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "DELETE FROM documents WHERE db=? AND collection=? AND id=?")) {
            statement.setString(1, dbName);
            statement.setString(2, collection);
            statement.setString(3, String.valueOf(id));
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite delete failed for " + collection, e);
        }
    }

    /** Indexes are implicit in the table definition, so this only exists for API parity. */
    public void ensureIndexes() {}

    /**
     * Writes only these fields, leaving the rest of the stored document untouched.
     *
     * <p>This is the SQLite replacement for the old field-level {@code $set}/{@code $unset}: read
     * the row, overwrite the named keys from the live object, write it back.
     */
    public synchronized void updateFields(Object entity, String... fields) {
        String collection = collectionOf(entity.getClass());
        if (collection == null) return;
        String id = idOf(entity);
        JsonObject stored = readJson(collection, id);
        if (stored == null) {
            save(entity);
            return;
        }
        JsonObject fresh = GSON.toJsonTree(entity).getAsJsonObject();
        for (String field : fields) {
            if (fresh.has(field) && !fresh.get(field).isJsonNull()) {
                stored.add(field, fresh.get(field));
            } else {
                stored.remove(field);
            }
        }
        writeJson(collection, id, stored);
    }

    private JsonObject readJson(String collection, String id) {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "SELECT doc FROM documents WHERE db=? AND collection=? AND id=?")) {
            statement.setString(1, dbName);
            statement.setString(2, collection);
            statement.setString(3, id);
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) return null;
                return JsonParser.parseString(rows.getString(1)).getAsJsonObject();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite read failed for " + collection + "/" + id, e);
        }
    }

    private void writeJson(String collection, String id, org.bson.Document document) {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "UPDATE documents SET doc=? WHERE db=? AND collection=? AND id=?")) {
            statement.setString(1, document.toJson());
            statement.setString(2, dbName);
            statement.setString(3, collection);
            statement.setString(4, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite update failed for " + collection + "/" + id, e);
        }
    }

    private void writeJson(String collection, String id, JsonObject json) {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "UPDATE documents SET doc=? WHERE db=? AND collection=? AND id=?")) {
            statement.setString(1, json.toString());
            statement.setString(2, dbName);
            statement.setString(3, collection);
            statement.setString(4, id);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite update failed for " + collection + "/" + id, e);
        }
    }

    /**
     * Reserves the next integer from a named counter, raising it to {@code floor} first.
     *
     * <p>The floor only ever moves a counter forward, matching the old Mongo behaviour: a counter
     * that fell behind already-created documents is repaired without ever going backwards.
     */
    public synchronized int nextCounter(String name, int floor) {
        if (floor < 0 || floor == Integer.MAX_VALUE) {
            throw new IllegalStateException("No more ids are available for " + name);
        }
        try (PreparedStatement insert =
                connection.prepareStatement(
                        "INSERT INTO counters (db, name, value) VALUES (?, ?, ?)"
                                + " ON CONFLICT(db, name) DO NOTHING")) {
            insert.setString(1, dbName);
            insert.setString(2, name);
            insert.setLong(3, floor);
            insert.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite counter insert failed for " + name, e);
        }
        try (PreparedStatement update =
                connection.prepareStatement(
                        "UPDATE counters SET value = CASE WHEN value < ? THEN ? ELSE value + 1 END"
                                + " WHERE db=? AND name=? RETURNING value")) {
            update.setLong(1, floor);
            update.setLong(2, floor);
            update.setString(3, dbName);
            update.setString(4, name);
            try (ResultSet rows = update.executeQuery()) {
                if (!rows.next()) {
                    throw new IllegalStateException("Could not reserve id from counter " + name);
                }
                long reserved = rows.getLong(1);
                if (reserved <= 0 || reserved > Integer.MAX_VALUE) {
                    throw new IllegalStateException("Id counter exhausted for " + name);
                }
                return (int) reserved;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite counter update failed for " + name, e);
        }
    }

    /** Cheap liveness check used by the watchdog; SQLite is local, so this is just SELECT 1. */
    public synchronized boolean ping() {
        try (Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery("SELECT 1")) {
            return rows.next();
        } catch (SQLException e) {
            return false;
        }
    }

    /** Claims a specific id for a reserved UID. Returns false when it is already taken. */
    public synchronized boolean claimId(String collection, String id) {
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "INSERT INTO documents (db, collection, id, doc) VALUES (?, ?, ?, '{}')")) {
            statement.setString(1, dbName);
            statement.setString(2, collection);
            statement.setString(3, id);
            statement.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    // ------------------------------------------------------------------ reads

    public <T> SqliteQuery<T> find(Class<T> type) {
        return new SqliteQuery<>(this, type);
    }

    /** Returns every stored document of a type, in insertion order. */
    <T> List<T> loadAll(Class<T> type) {
        String collection = collectionOf(type);
        if (collection == null) return List.of();
        List<T> result = new ArrayList<>();
        try (PreparedStatement statement =
                connection.prepareStatement(
                        "SELECT doc FROM documents WHERE db=? AND collection=? ORDER BY rowid")) {
            statement.setString(1, dbName);
            statement.setString(2, collection);
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    T parsed = GSON.fromJson(rows.getString(1), type);
                    if (parsed != null) result.add(parsed);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("SQLite read failed for " + collection, e);
        }
        return result;
    }

    /** Loads the subset matching a single field equality, pushed down into SQL. */
    <T> List<T> loadByField(Class<T> type, String field, Object value) {
        String collection = collectionOf(type);
        if (collection == null) return List.of();
        List<T> result = new ArrayList<>();
        String sql =
                "SELECT doc FROM documents WHERE db=? AND collection=?"
                        + " AND CAST(json_extract(doc, '$."
                        + escapePath(field)
                        + "') AS TEXT) = ? ORDER BY rowid";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, dbName);
            statement.setString(2, collection);
            statement.setString(3, String.valueOf(value));
            try (ResultSet rows = statement.executeQuery()) {
                while (rows.next()) {
                    T parsed = GSON.fromJson(rows.getString(1), type);
                    if (parsed != null) result.add(parsed);
                }
            }
        } catch (SQLException e) {
            // A type SQLite cannot compare (a nested object, for instance) falls back to a full scan.
            return loadAll(type).stream().filter(matchesField(field, value)).toList();
        }
        return result;
    }

    private static Predicate<Object> matchesField(String field, Object value) {
        return entity -> {
            Object actual = readField(entity, field);
            if (actual == null) return value == null;
            return String.valueOf(actual).equals(String.valueOf(value));
        };
    }

    private static String escapePath(String field) {
        return field.replace("'", "''").replace("\"", "");
    }

    // ------------------------------------------------------------------ helpers

    static String collectionOf(Class<?> type) {
        Entity annotation = type.getAnnotation(Entity.class);
        if (annotation == null || annotation.value().isEmpty()) return null;
        return annotation.value();
    }

    static String idOf(Object entity) {
        Field idField = idFieldOf(entity.getClass());
        if (idField == null) {
            throw new IllegalArgumentException("No @Id field on " + entity.getClass());
        }
        try {
            idField.setAccessible(true);
            Object value = idField.get(entity);
            if (value == null) {
                throw new IllegalArgumentException("@Id is null on " + entity.getClass());
            }
            if (value instanceof ObjectId objectId) return objectId.toHexString();
            return String.valueOf(value);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Could not read @Id on " + entity.getClass(), e);
        }
    }

    static Field idFieldOf(Class<?> type) {
        for (Class<?> current = type; current != null && current != Object.class; current = current.getSuperclass()) {
            for (Field field : current.getDeclaredFields()) {
                if (field.getAnnotation(Id.class) != null) return field;
            }
        }
        return null;
    }

    static Object readField(Object entity, String name) {
        if (entity instanceof org.bson.Document document) return document.get(name);
        // Morphia calls the primary key "_id"; the JSON uses the Java field name behind @Id.
        if ("_id".equals(name)) {
            Field idField = idFieldOf(entity.getClass());
            if (idField != null) name = idField.getName();
        }
        for (Class<?> current = entity.getClass(); current != null && current != Object.class; current = current.getSuperclass()) {
            try {
                Field field = current.getDeclaredField(name);
                field.setAccessible(true);
                return field.get(entity);
            } catch (NoSuchFieldException ignored) {
                // Keep walking up the hierarchy.
            } catch (IllegalAccessException e) {
                return null;
            }
        }
        return null;
    }

    /**
     * Flattens the small set of Morphia filters the codebase actually uses into a predicate.
     *
     * <p>Anything unsupported is treated as "match everything" rather than silently dropping rows;
     * the caller's own post-filtering still applies where it exists.
     */
    static Predicate<Object> toPredicate(Bson filter) {
        if (filter == null) return entity -> true;
        BsonDocument document;
        try {
            document = filter.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
        } catch (RuntimeException e) {
            return entity -> true;
        }
        return predicateFor(document);
    }

    private static Predicate<Object> predicateFor(BsonDocument document) {
        if (document.containsKey("$and")) {
            List<Predicate<Object>> parts = new ArrayList<>();
            for (BsonValue value : document.getArray("$and")) {
                parts.add(predicateFor(value.asDocument()));
            }
            return entity -> parts.stream().allMatch(p -> p.test(entity));
        }
        if (document.containsKey("$or")) {
            List<Predicate<Object>> parts = new ArrayList<>();
            for (BsonValue value : document.getArray("$or")) {
                parts.add(predicateFor(value.asDocument()));
            }
            return entity -> parts.isEmpty() || parts.stream().anyMatch(p -> p.test(entity));
        }
        if (document.containsKey("$nor")) {
            List<Predicate<Object>> parts = new ArrayList<>();
            for (BsonValue value : document.getArray("$nor")) {
                parts.add(predicateFor(value.asDocument()));
            }
            return entity -> parts.stream().noneMatch(p -> p.test(entity));
        }
        List<Predicate<Object>> parts = new ArrayList<>();
        for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
            String field = entry.getKey();
            if (field.startsWith("$")) continue;
            parts.add(predicateForField(field, entry.getValue()));
        }
        return entity -> parts.stream().allMatch(p -> p.test(entity));
    }

    private static Predicate<Object> predicateForField(String field, BsonValue condition) {
        if (condition.isDocument()) {
            BsonDocument operators = condition.asDocument();
            if (operators.containsKey("$regex")) {
                String pattern = operators.getString("$regex").getValue();
                java.util.regex.Pattern compiled = java.util.regex.Pattern.compile(pattern);
                return entity -> {
                    Object actual = readField(entity, field);
                    return actual != null && compiled.matcher(String.valueOf(actual)).find();
                };
            }
            if (operators.containsKey("$in")) {
                Set<String> allowed = new HashSet<>();
                for (BsonValue value : operators.getArray("$in")) {
                    allowed.add(bsonToString(value));
                }
                return entity -> {
                    Object actual = readField(entity, field);
                    return actual != null && allowed.contains(String.valueOf(actual));
                };
            }
            if (operators.containsKey("$ne")) {
                String notEqual = bsonToString(operators.get("$ne"));
                return entity -> {
                    Object actual = readField(entity, field);
                    return actual == null || !String.valueOf(actual).equals(notEqual);
                };
            }
            if (operators.containsKey("$exists")) {
                boolean exists = operators.getBoolean("$exists").getValue();
                return entity -> (readField(entity, field) != null) == exists;
            }
            // Nested document equality, e.g. a position object.
            String expected = operators.toJson();
            return entity -> {
                Object actual = readField(entity, field);
                return actual != null && GSON.toJson(actual).equals(expected);
            };
        }
        String expected = bsonToString(condition);
        return entity -> {
            Object actual = readField(entity, field);
            if (actual == null) return expected == null || "null".equals(expected);
            if (actual instanceof Number number && isNumeric(expected)) {
                return Double.compare(number.doubleValue(), Double.parseDouble(expected)) == 0;
            }
            return String.valueOf(actual).equals(expected);
        };
    }

    private static boolean isNumeric(String value) {
        if (value == null || value.isEmpty()) return false;
        try {
            Double.parseDouble(value);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static String bsonToString(BsonValue value) {
        if (value == null || value.isNull()) return null;
        if (value.isString()) return value.asString().getValue();
        if (value.isInt32()) return Integer.toString(value.asInt32().getValue());
        if (value.isInt64()) return Long.toString(value.asInt64().getValue());
        if (value.isDouble()) return Double.toString(value.asDouble().getValue());
        if (value.isBoolean()) return Boolean.toString(value.asBoolean().getValue());
        if (value.isObjectId()) return value.asObjectId().getValue().toHexString();
        return value.toString();
    }

    @Override
    public synchronized void close() {
        try {
            connection.close();
        } catch (SQLException ignored) {
            // Shutting down anyway.
        }
    }

    /** A tiny stand-in for Morphia's query builder covering first/count/stream/iterator. */
    public static final class SqliteQuery<T> {
        private final SqliteDatastore store;
        private final Class<T> type;
        private List<Object> filters = List.of();
        private int limit = -1;

        SqliteQuery(SqliteDatastore store, Class<T> type) {
            this.store = store;
            this.type = type;
        }

        /**
         * Accepts either a BSON filter or a Morphia {@code Filter}.
         *
         * <p>Morphia's Filter is not a Bson implementation, but it exposes the field, operator and
         * value the predicate needs, so both vocabularies work without rewriting call sites.
         */
        public SqliteQuery<T> filter(Object... filters) {
            if (filters == null || filters.length == 0) return this;
            this.filters = new ArrayList<>(List.of(filters));
            return this;
        }

        public SqliteQuery<T> limit(int limit) {
            this.limit = limit;
            return this;
        }

        public List<T> list() {
            List<T> loaded = load();
            if (limit >= 0 && loaded.size() > limit) return loaded.subList(0, limit);
            return loaded;
        }

        public T first() {
            List<T> loaded = list();
            return loaded.isEmpty() ? null : loaded.get(0);
        }

        public long count() {
            return list().size();
        }

        public Stream<T> stream() {
            return list().stream();
        }

        public Iterator<T> iterator() {
            return list().iterator();
        }

        /** Deletes every match and returns how many were removed. */
        public long delete() {
            List<T> matches = list();
            for (T entity : matches) {
                store.delete(entity);
            }
            return matches.size();
        }

        private List<T> load() {
            if (filters.isEmpty()) {
                return store.loadAll(type);
            }
            Predicate<Object> predicate = combinedPredicate(filters);
            List<T> candidates = store.loadAll(type).stream().filter(predicate).toList();
            if (limit >= 0 && candidates.size() > limit) {
                return candidates.subList(0, limit);
            }
            return candidates;
        }

        private static Predicate<Object> combinedPredicate(List<Object> filters) {
            List<Predicate<Object>> parts = new ArrayList<>();
            for (Object filter : filters) {
                Predicate<Object> predicate = predicateOf(filter);
                if (predicate != null) parts.add(predicate);
            }
            return entity -> parts.stream().allMatch(p -> p.test(entity));
        }

    }

    /** Turns one Morphia filter or Bson filter into a predicate over a loaded entity. */
    @SuppressWarnings("unchecked")
    static Predicate<Object> predicateOf(Object filter) {
        if (filter == null) return null;
        if (filter instanceof Bson bson) {
            return toPredicate(bson);
        }
        if (!(filter instanceof dev.morphia.query.experimental.filters.Filter morphia)) {
            return null;
        }
        String name = morphia.getName();
        if ("$and".equals(name) || "$or".equals(name) || "$nor".equals(name)) {
            List<dev.morphia.query.experimental.filters.Filter> children = logicalChildren(morphia);
            List<Predicate<Object>> parts = new ArrayList<>();
            for (var child : children) {
                Predicate<Object> predicate = predicateOf(child);
                if (predicate != null) parts.add(predicate);
            }
            return switch (name) {
                case "$or" -> entity -> parts.stream().anyMatch(p -> p.test(entity));
                case "$nor" -> entity -> parts.stream().noneMatch(p -> p.test(entity));
                default -> entity -> parts.stream().allMatch(p -> p.test(entity));
            };
        }
        String field = morphia.getField();
        Object value = morphia.getValue();
        return switch (name) {
            case "$eq" -> entity -> fieldEquals(entity, field, value);
            case "$ne" -> entity -> !fieldEquals(entity, field, value);
            case "$in" -> entity -> valueContains(value, readField(entity, field));
            case "$nin" -> entity -> !valueContains(value, readField(entity, field));
            case "$exists" -> entity -> (readField(entity, field) != null) == Boolean.TRUE.equals(value);
            case "$regex" -> regexPredicate(field, value);
            default -> null;
        };
    }

    @SuppressWarnings("unchecked")
    private static List<dev.morphia.query.experimental.filters.Filter> logicalChildren(
            dev.morphia.query.experimental.filters.Filter filter) {
        try {
            Field field = filter.getClass().getDeclaredField("filters");
            field.setAccessible(true);
            return (List<dev.morphia.query.experimental.filters.Filter>) field.get(filter);
        } catch (ReflectiveOperationException e) {
            return List.of();
        }
    }

    private static boolean fieldEquals(Object entity, String field, Object expected) {
        Object actual = readField(entity, field);
        if (actual == null) return expected == null;
        if (expected == null) return false;
        if (actual instanceof Number number && expected instanceof Number wanted) {
            return Double.compare(number.doubleValue(), wanted.doubleValue()) == 0;
        }
        if (actual instanceof Number number) {
            try {
                return Double.compare(number.doubleValue(), Double.parseDouble(String.valueOf(expected))) == 0;
            } catch (NumberFormatException ignored) {
                // Fall through to a text comparison.
            }
        }
        return String.valueOf(actual).equals(String.valueOf(expected));
    }

    private static boolean valueContains(Object candidates, Object actual) {
        if (candidates == null || actual == null) return false;
        if (candidates instanceof Iterable<?> iterable) {
            for (Object candidate : iterable) {
                if (String.valueOf(candidate).equals(String.valueOf(actual))) return true;
            }
            return false;
        }
        if (candidates.getClass().isArray()) {
            int length = java.lang.reflect.Array.getLength(candidates);
            for (int i = 0; i < length; i++) {
                if (String.valueOf(java.lang.reflect.Array.get(candidates, i)).equals(String.valueOf(actual))) return true;
            }
            return false;
        }
        return String.valueOf(candidates).equals(String.valueOf(actual));
    }

    private static Predicate<Object> regexPredicate(String field, Object value) {
        java.util.regex.Pattern pattern;
        if (value instanceof java.util.regex.Pattern compiled) {
            pattern = compiled;
        } else {
            try {
                pattern = java.util.regex.Pattern.compile(String.valueOf(value));
            } catch (RuntimeException e) {
                return entity -> true;
            }
        }
        return entity -> {
            Object actual = readField(entity, field);
            return actual != null && pattern.matcher(String.valueOf(actual)).find();
        };
    }

    /** Convenience for callers that want a plain stream over a type. */
    public static <T> Stream<T> streamOf(SqliteDatastore store, Class<T> type) {
        return StreamSupport.stream(store.loadAll(type).spliterator(), false);
    }

    // ------------------------------------------------------------------ raw documents

    /**
     * Raw document access, standing in for {@code MongoDatabase.getCollection(...)}.
     *
     * <p>Documents are stored as BSON JSON under their {@code _id}, so hand-written callers that
     * worked against Mongo keep working with the same Document/Filter/Updates vocabulary.
     */
    public RawCollection collection(String name) {
        return new RawCollection(name);
    }

    public final class RawCollection {
        private final String name;

        private RawCollection(String name) {
            this.name = name;
        }

        public void insertOne(org.bson.Document document) {
            String id = documentId(document);
            String json = document.toJson();
            synchronized (SqliteDatastore.this) {
                try (PreparedStatement statement =
                        connection.prepareStatement(
                                "INSERT INTO documents (db, collection, id, doc) VALUES (?, ?, ?, ?)")) {
                    statement.setString(1, dbName);
                    statement.setString(2, name);
                    statement.setString(3, id);
                    statement.setString(4, json);
                    statement.executeUpdate();
                } catch (SQLException e) {
                    throw new IllegalStateException("Duplicate key " + name + "/" + id, e);
                }
            }
        }

        public void replaceOne(
                Bson filter,
                org.bson.Document replacement,
                com.mongodb.client.model.ReplaceOptions options) {
            replaceOne(filter, replacement);
        }

        public void replaceOne(Bson filter, org.bson.Document replacement) {
            org.bson.Document existing = find(filter).first();
            if (existing == null) {
                insertOne(replacement);
                return;
            }
            synchronized (SqliteDatastore.this) {
                writeJson(name, documentId(existing), org.bson.Document.parse(replacement.toJson()));
            }
        }

        public RawFind find() {
            return new RawFind(this, null);
        }

        public RawFind find(Bson filter) {
            return new RawFind(this, filter);
        }

        public RawUpdateResult updateOne(Bson filter, Bson update) {
            org.bson.Document target = find(filter).first();
            if (target == null) return new RawUpdateResult(0);
            BsonDocument updateDocument;
            try {
                updateDocument =
                        update.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
            } catch (RuntimeException e) {
                return new RawUpdateResult(0);
            }
            for (Map.Entry<String, BsonValue> entry : updateDocument.entrySet()) {
                if ("$set".equals(entry.getKey())) {
                    org.bson.Document values = org.bson.Document.parse(entry.getValue().asDocument().toJson());
                    target.putAll(values);
                } else if ("$unset".equals(entry.getKey())) {
                    for (String key : entry.getValue().asDocument().keySet()) target.remove(key);
                } else if ("$inc".equals(entry.getKey())) {
                    org.bson.Document values = org.bson.Document.parse(entry.getValue().asDocument().toJson());
                    for (Map.Entry<String, Object> increment : values.entrySet()) {
                        Object current = target.get(increment.getKey());
                        if (current instanceof Number number && increment.getValue() instanceof Number delta) {
                            target.put(increment.getKey(), number.longValue() + delta.longValue());
                        }
                    }
                }
            }
            synchronized (SqliteDatastore.this) {
                writeJson(name, documentId(target), target);
            }
            return new RawUpdateResult(1);
        }

        public long deleteMany(Bson filter) {
            Predicate<Object> predicate = toPredicate(filter);
            List<org.bson.Document> matches =
                    loadRaw(name).stream()
                            .filter(document -> predicate.test(document))
                            .toList();
            for (org.bson.Document document : matches) {
                synchronized (SqliteDatastore.this) {
                    try (PreparedStatement statement =
                            connection.prepareStatement(
                                    "DELETE FROM documents WHERE db=? AND collection=? AND id=?")) {
                        statement.setString(1, dbName);
                        statement.setString(2, name);
                        statement.setString(3, documentId(document));
                        statement.executeUpdate();
                    } catch (SQLException e) {
                        throw new IllegalStateException("SQLite delete failed for " + name, e);
                    }
                }
            }
            return matches.size();
        }

        public long deleteOne(Bson filter) {
            org.bson.Document target = find(filter).first();
            if (target == null) return 0;
            synchronized (SqliteDatastore.this) {
                try (PreparedStatement statement =
                        connection.prepareStatement(
                                "DELETE FROM documents WHERE db=? AND collection=? AND id=?")) {
                    statement.setString(1, dbName);
                    statement.setString(2, name);
                    statement.setString(3, documentId(target));
                    statement.executeUpdate();
                } catch (SQLException e) {
                    throw new IllegalStateException("SQLite delete failed for " + name, e);
                }
            }
            return 1;
        }

        public long countDocuments(Bson filter) {
            return find(filter).into(new ArrayList<>()).size();
        }

        List<org.bson.Document> loadRaw(String collection) {
            List<org.bson.Document> result = new ArrayList<>();
            synchronized (SqliteDatastore.this) {
                try (PreparedStatement statement =
                        connection.prepareStatement(
                                "SELECT doc FROM documents WHERE db=? AND collection=? ORDER BY rowid")) {
                    statement.setString(1, dbName);
                    statement.setString(2, collection);
                    try (ResultSet rows = statement.executeQuery()) {
                        while (rows.next()) {
                            result.add(org.bson.Document.parse(rows.getString(1)));
                        }
                    }
                } catch (SQLException e) {
                    throw new IllegalStateException("SQLite read failed for " + collection, e);
                }
            }
            return result;
        }
    }

    private static String documentId(org.bson.Document document) {
        Object id = document.get("_id");
        if (id == null) {
            throw new IllegalArgumentException("Raw document has no _id");
        }
        if (id instanceof ObjectId objectId) return objectId.toHexString();
        return String.valueOf(id);
    }

    /** The subset of UpdateResult the callers read. */
    public static final class RawUpdateResult {
        private final long modified;

        RawUpdateResult(long modified) {
            this.modified = modified;
        }

        public long getModifiedCount() {
            return modified;
        }

        public long getMatchedCount() {
            return modified;
        }
    }

    /** The subset of FindIterable the callers read. */
    public final class RawFind implements Iterable<org.bson.Document> {
        private final RawCollection collection;
        private Bson filter;
        private int limit = -1;

        RawFind(RawCollection collection, Bson filter) {
            this.collection = collection;
            this.filter = filter;
        }

        public RawFind filter(Bson filter) {
            this.filter = filter;
            return this;
        }

        public RawFind limit(int limit) {
            this.limit = limit;
            return this;
        }

        public org.bson.Document first() {
            List<org.bson.Document> all = into(new ArrayList<>());
            return all.isEmpty() ? null : all.get(0);
        }

        public List<org.bson.Document> into(List<org.bson.Document> target) {
            Predicate<Object> predicate = toPredicate(filter);
            List<org.bson.Document> matches =
                    collection.loadRaw(collectionName()).stream()
                            .filter(document -> predicate.test(document))
                            .toList();
            if (limit >= 0 && matches.size() > limit) matches = matches.subList(0, limit);
            target.addAll(matches);
            return target;
        }

        private String collectionName() {
            try {
                Field field = RawCollection.class.getDeclaredField("name");
                field.setAccessible(true);
                return (String) field.get(collection);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException("Could not read raw collection name", e);
            }
        }

        @Override
        public Iterator<org.bson.Document> iterator() {
            return into(new ArrayList<>()).iterator();
        }
    }
}

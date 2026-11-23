package emu.grasscutter.database;

import static emu.grasscutter.config.Configuration.DATABASE;

import emu.grasscutter.Grasscutter;
import emu.grasscutter.Grasscutter.ServerRunMode;
import java.io.File;

/**
 * Opens the SQLite stores and hands out ids.
 *
 * <p>Both stores are plain SQLite files. {@code game} holds players and everything they own,
 * {@code server} holds accounts and counters when running dispatch and game separately. In hybrid
 * mode a single file serves both.
 */
public final class DatabaseManager {
    private static final int DEFAULT_COUNTER_START = 10_000;
    private static final String PLAYER_UID_COUNTER = "PlayerUid";
    private static final String PLAYER_UID_RESERVATIONS = "player_uid_reservations";
    private static final String DEFAULT_PATH = "data/grasscutter.db";

    private static SqliteDatastore gameDatastore;
    private static SqliteDatastore dispatchDatastore;

    private DatabaseManager() {}

    public static SqliteDatastore getGameDatastore() {
        return gameDatastore;
    }

    public static SqliteDatastore getAccountDatastore() {
        if (Grasscutter.getRunMode() == ServerRunMode.HYBRID) return gameDatastore;
        return dispatchDatastore;
    }

    public static void initialize() {
        gameDatastore = open("game", DATABASE.game.connectionUri);
        if (Grasscutter.getRunMode() != ServerRunMode.HYBRID) {
            dispatchDatastore = open("server", DATABASE.server.connectionUri);
        }
    }

    public static void close() {
        if (gameDatastore != null) gameDatastore.close();
        if (dispatchDatastore != null) dispatchDatastore.close();
    }

    private static SqliteDatastore open(String name, String configured) {
        String path = configured == null || configured.isBlank() ? DEFAULT_PATH : configured;
        // A config left over from the Mongo build still points at mongodb://. Use the SQLite file
        // rather than refusing to start, so upgrading an existing install needs no manual edit.
        if (path.startsWith("mongodb://") || path.startsWith("mongodb+srv://")) {
            path = DEFAULT_PATH;
        }
        File file = new File(path).getAbsoluteFile();
        File parent = file.getParentFile();
        if (parent != null && !parent.exists() && !parent.mkdirs()) {
            Grasscutter.getLogger().warn("Could not create database directory {}", parent);
        }
        Grasscutter.getLogger().info("Database ({}): {}", name, file.getPath());
        return new SqliteDatastore(name, file.getPath());
    }

    /**
     * Reserves and returns the next integer from a named counter.
     *
     * <p>The increment happens inside SQLite before this method returns. The floor only moves a
     * stale counter forward; it never moves one backwards.
     */
    static int reserveNextId(SqliteDatastore datastore, String counterName, int floor) {
        return DatabaseHelper.callSynchronousDatabaseWrite(
                () -> datastore.nextCounter(counterName, floor));
    }

    public static int getNextId(Class<?> c) {
        return reserveNextId(getGameDatastore(), c.getSimpleName(), DEFAULT_COUNTER_START);
    }

    public static int getNextId(Object o) {
        return getNextId(o.getClass());
    }

    static int reserveNextPlayerUid(int highestExistingUid) {
        return reserveNextId(
                getGameDatastore(), PLAYER_UID_COUNTER, Math.max(0, highestExistingUid));
    }

    static boolean tryReservePlayerUid(int uid) {
        return DatabaseHelper.callSynchronousDatabaseWrite(
                () -> getGameDatastore().claimId(PLAYER_UID_RESERVATIONS, String.valueOf(uid)));
    }
}

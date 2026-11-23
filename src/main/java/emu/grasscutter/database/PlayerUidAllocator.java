package emu.grasscutter.database;

import emu.grasscutter.game.player.Player;
import java.util.Comparator;
import java.util.function.Consumer;

/** Allocates player UIDs without assuming that the historic 10,001 counter floor is free. */
public final class PlayerUidAllocator {
    private static final PlayerUidReservation.Store DATABASE_STORE =
            new PlayerUidReservation.Store() {
                @Override
                public int highestUid() {
                    return DatabaseManager.getGameDatastore().find(Player.class).list().stream()
                            .max(Comparator.comparingInt(Player::getUid))
                            .map(Player::getUid)
                            .orElse(0);
                }

                @Override
                public boolean playerExists(int uid) {
                    return DatabaseHelper.checkIfPlayerExists(uid);
                }

                @Override
                public boolean tryReserve(int uid) {
                    return DatabaseManager.tryReservePlayerUid(uid);
                }

                @Override
                public int reserveNext(int floor) {
                    return DatabaseManager.reserveNextPlayerUid(floor);
                }
            };

    private PlayerUidAllocator() {}

    public static int next(int reservedUid) {
        return PlayerUidReservation.next(reservedUid, DATABASE_STORE);
    }

    /** Persists a newly-created player with insert semantics; duplicate keys must stay failures. */
    public static void assignReserved(Player player, int reservedUid) {
        assignReserved(
                player,
                reservedUid,
                value -> DatabaseHelper.runSynchronousDatabaseWrite(
                        () -> DatabaseManager.getGameDatastore().insert(value)));
    }

    static void assignReserved(Player player, int reservedUid, Consumer<Player> insert) {
        if (reservedUid < PlayerUidReservation.FIRST_UID) {
            throw new IllegalArgumentException("A player UID must be positive");
        }
        player.setUid(reservedUid);
        insert.accept(player);
    }
}

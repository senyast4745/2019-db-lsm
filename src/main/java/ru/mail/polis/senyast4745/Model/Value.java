package ru.mail.polis.senyast4745.Model;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public final class Value implements Comparable<Value> {

    private final ByteBuffer data;
    private final long timestamp;
    private final boolean tombstone;

    public long getTimestamp() {
        return timestamp;
    }

    public ByteBuffer getData() {
        return data.asReadOnlyBuffer();
    }

    public static Value of(@NotNull final ByteBuffer data) {
        return new Value(data, System.currentTimeMillis(), false);
    }

    public static Value tombstone() {
        return new Value(null, System.currentTimeMillis(), true);
    }

    public Value(final ByteBuffer data, final long timestamp, final boolean isDead) {
        this.data = data;
        this.timestamp = timestamp;
        this.tombstone = isDead;
    }

    public boolean isTombstone() {
        return tombstone;
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return -Long.compare(timestamp, o.timestamp);
    }
}

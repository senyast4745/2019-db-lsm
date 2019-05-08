package ru.mail.polis.senyast4745;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

public final class Value implements Comparable<Value> {
    private final long ts;
    private final ByteBuffer data;
    private static final AtomicInteger atomicInteger = new AtomicInteger();

    private Value(final long ts, final ByteBuffer data) {
        assert ts >= 0;
        this.ts = ts;
        this.data = data;
    }

    public static Value of(final ByteBuffer data) {
        return new Value(getTime(), data.duplicate());
    }

    public static Value of(final long time, final ByteBuffer data) {
        return new Value(time, data.duplicate());
    }

    static Value tombstone() {
        return tombstone(System.currentTimeMillis());
    }

    static Value tombstone(final long time) {
        return new Value(time, null);
    }

    boolean isRemoved() {
        return data == null;
    }

    ByteBuffer getData() {
        if (data == null) {
            throw new IllegalArgumentException("Value data is null");
        }
        return data.asReadOnlyBuffer();
    }

    @Override
    public int compareTo(@NotNull final Value o) {
        return Long.compare(o.ts, ts);
    }

    long getTimeStamp() {
        return ts;
    }

    private static long getTime() {
        final long time = System.currentTimeMillis() * 1000 + atomicInteger.incrementAndGet();
        if (atomicInteger.get() > 1000) atomicInteger.set(0);
        return time;
    }
}

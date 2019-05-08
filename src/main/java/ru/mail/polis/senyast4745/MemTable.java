package ru.mail.polis.senyast4745;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemTable implements Table {
    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();
    private long size;

    @Override
    public long sizeInBytes() {
        return size;
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(
                map.tailMap(from).entrySet().iterator(),
                e -> {
                    assert e != null;
                    return new Cell(e.getKey(), e.getValue());

                });
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        final Value previous = map.put(key, Value.of(value));
        if (previous == null) {
            size += key.remaining() + value.remaining();
        } else if (previous.isRemoved()) {
            size += value.remaining();
        } else {
            size += value.remaining() - previous.getData().remaining();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        final Value previous = map.put(key, Value.tombstone());
        if (previous == null) {
            size += key.remaining();
        } else if (!previous.isRemoved()) {
            size -= previous.getData().remaining();
        }
    }



}

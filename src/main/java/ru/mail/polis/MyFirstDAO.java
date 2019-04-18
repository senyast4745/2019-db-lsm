package ru.mail.polis;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;

public class MyFirstDAO implements DAO {

    private final NavigableMap<ByteBuffer, Record> db = new TreeMap<>();

    @Override
    public @NotNull
    Iterator<Record> iterator(@NotNull ByteBuffer from) {
        return db.tailMap(from).values().iterator();
    }

    @Override
    public void upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value) {
        db.put(key, Record.of(key, value));
    }

    @Override
    public void remove(@NotNull ByteBuffer key) {
        db.remove(key);
    }

    @Override
    public void close()  {
        //Do nothing
    }
}

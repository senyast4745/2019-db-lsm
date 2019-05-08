package ru.mail.polis.senyast4745;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Cell {
    static final Comparator<Cell> COMPARATOR =
            Comparator.comparing(Cell::getKey).thenComparing(Cell::getValue);

    private final ByteBuffer key;
    private final Value value;

    Cell(ByteBuffer key, Value value) {
        this.key = key;
        this.value = value;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }
}

package ru.mail.polis.senyast4745.Model;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class Cell {
    public static final Comparator<Cell> COMPARATOR =
            Comparator.comparing(Cell::getKey).thenComparing(Cell::getValue)
                    .thenComparing(Cell::getGeneration, Comparator.reverseOrder());

    private final ByteBuffer key;
    private final Value value;
    private final long  generation;

    public Cell(final ByteBuffer key, final Value value, final long generation) {
        this.key = key;
        this.value = value;
        this.generation = generation;
    }

    public ByteBuffer getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }

    private long getGeneration() {
        return generation;
    }
}

package ru.mail.polis.senyast4745;


import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;

import com.google.common.collect.Iterators;

import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

public class LSMDao implements DAO {
    private static final String TABLE_NAME = "SSTable";
    private static final String SUFFIX = ".dat";
    private static final String TEMP = ".tmp";

    private Table memTable = new MemTable();
    private final long flushThreshold;
    private final File base;
    private int generation;
    private List<FileTable> fileTables;

    public LSMDao(final File base, final long flushThreshold) throws IOException {
        this.base = base;
        assert flushThreshold >= 0L;
        this.flushThreshold = flushThreshold;
        readFiles();
    }

    private void readFiles() throws IOException {
        Stream<Path> stream = Files.walk(base.toPath(), 1).filter(path -> path.getFileName().toString().endsWith(SUFFIX));
        final List<Path> files = stream.collect(Collectors.toList());

        fileTables = new ArrayList<>(files.size());
        generation = -1;
        files.forEach(path -> {
            File file = path.toFile();
            try {
                FileTable fileTable = new FileTable(file);
                fileTables.add(fileTable);
                generation = Math.max(generation, FileTable.getGenerationByName(file.getName()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        generation++;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> list = new ArrayList<>(fileTables.size() + 1);
        fileTables.forEach(fileTable -> list.add(fileTable.iterator(from)));

        final Iterator<Cell> cells = memTable.iterator(from);
        list.add(cells);

        Iterator<Cell> iterator = Iterators.mergeSorted(list, Cell.COMPARATOR);

        iterator = Iters.collapseEquals(iterator, Cell::getKey);

        final Iterator<Cell> alive =
                Iterators.filter(
                        iterator,
                        cell -> cell == null || cell.getValue() == null || !cell.getValue().isRemoved());

        return Iterators.transform(
                alive,
                cell -> {
                    assert cell != null && cell.getKey() != null;
                    return Record.of(cell.getKey(), cell.getValue().getData());
                });
    }


    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value);
        if (memTable.sizeInBytes() > flushThreshold) {
            flush();
            readFiles();
        }
    }

    private void flush() throws IOException {
        flush(memTable.iterator(ByteBuffer.allocate(0)), generation++);
        memTable.clear();
    }

    private void flush(Iterator<Cell> iterator, int generation) throws IOException {
        final File tmp = new File(base, TABLE_NAME + generation + TEMP);
        FileTable.write(iterator, tmp);
        final File dest = new File(base, TABLE_NAME + generation + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);

        /*final File tmp = new File(base, TABLE_NAME + generation + SUFFIX);
        FileTable.write(iterator, tmp);*/
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
    }/*final File tmp = new File(base, TABLE_NAME + generation + SUFFIX);
        FileTable.write(iterator, tmp);*/

    private void mergeTables(int from, int to) throws IOException {
        List<FileTable> mergeFiles = fileTables.subList(from, to);
        fileTables = fileTables.subList(0, from);

        Iterator<Cell> mergeIterator = FileTable.merge(mergeFiles);
        int generation = -1;
        for (FileTable table : mergeFiles) {
            File file = table.getFile();
            String name = file.getName();
            generation = Math.max(generation, FileTable.getGenerationByName(name));
        }

        if (generation >= 0) {
            flush(mergeIterator, generation);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        mergeTables(fileTables.size() / 2, fileTables.size());
    }
}
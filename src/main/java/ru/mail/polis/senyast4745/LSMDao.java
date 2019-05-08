package ru.mail.polis.senyast4745;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Main DAO
 */
public class LSMDao implements DAO {
    private static final String TEMP = ".tmp";
    private static final String SUFFIX = ".dat";
    private static final String TABLE_NAME = "SSTable";

    private Table memTable = new MemTable();
    private final long flushLimit;
    private final File base;
    private int generation;
    private final List<FileTable> fileTables;

    public LSMDao(final File base, final long flushMax) throws IOException {
        this.base = base;
        assert flushMax >= 0L;
        this.flushLimit = flushMax;
        fileTables = new ArrayList<>();
        Files.walkFileTree(base.toPath(), new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                fileTables.add(new FileTable(file.toFile()));
                return FileVisitResult.CONTINUE;
            }
        });
    }


    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) throws IOException {
        final List<Iterator<Cell>> iters = new ArrayList<>();
        for (final FileTable fileTable : this.fileTables) {
            iters.add(fileTable.iterator(from));
        }

        iters.add(memTable.iterator(from));
        final Iterator<Cell> cellIterator = Iters.collapseEquals(
                Iterators.mergeSorted(iters, Cell.COMPARATOR),
                Cell::getKey
        );
        final Iterator<Cell> alive = Iterators.filter(
                cellIterator, cell -> {
                    assert cell != null;
                    return !cell.getValue().isRemoved();
                }
        );
        return Iterators.transform(alive, cell -> {
            assert cell != null;
            return Record.of(cell.getKey(), cell.getValue().getData());
        });
    }


    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        memTable.upsert(key.duplicate(), value);
        if (memTable.sizeInBytes() >= flushLimit) {
            flush();
        }
    }


    private void flush() throws IOException {
        final File tmp = new File(base,  TABLE_NAME + generation + TEMP);
        FileTable.write(memTable.iterator(ByteBuffer.allocate(0)), tmp);
        final File dest = new File(base, TABLE_NAME + generation + SUFFIX);
        Files.move(tmp.toPath(), dest.toPath(), StandardCopyOption.ATOMIC_MOVE);
        generation++;
        memTable = new MemTable();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        memTable.remove(key);
    }


    @Override
    public void close() throws IOException {
        flush();
    }



}
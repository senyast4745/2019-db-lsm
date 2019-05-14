package ru.mail.polis.senyast4745.Model;

import ru.mail.polis.senyast4745.LSMDao;

import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Generation {

    private Generation(){}

    public static long fromPath(final Path path){
        return fromFileName(path.getFileName().toString());
    }

    /**
     * Get generation FileTable from filename.
     *
     * @param fileName name of FileTable file
     * @return generation number
     */
    private static long fromFileName(final String fileName){
        String pattern = LSMDao.PREFIX_FILE + "(\\d+)" + LSMDao.SUFFIX_DAT;
        final Pattern regex = Pattern.compile(pattern);
        final Matcher matcher = regex.matcher(fileName);
        if (matcher.find()){
            return Long.parseLong(matcher.group(1));
        }
        return -1L;
    }
}
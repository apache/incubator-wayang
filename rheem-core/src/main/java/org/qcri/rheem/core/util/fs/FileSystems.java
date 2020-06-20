package org.qcri.rheem.core.util.fs;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.util.LruCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

/**
 * Tool to work with {@link FileSystem}s.
 */
public class FileSystems {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystems.class);

    /**
     * We need file sizes several times during the optimization process, so we cache them.
     */
    private static final LruCache<String, Long> fileSizeCache = new LruCache<>(20);

    private static Collection<FileSystem> registeredFileSystems = Arrays.asList(
            new LocalFileSystem(),
            new HadoopFileSystem()
    );

    private FileSystems() {
    }


    public static Optional<FileSystem> getFileSystem(String fileUrl) {
        return registeredFileSystems.stream()
                .filter(fileSystem -> fileSystem.canHandle(fileUrl))
                .findAny();
    }

    public static FileSystem requireFileSystem(String fileUrl) {
        return getFileSystem(fileUrl).orElseThrow(
                () -> new RheemException(String.format("Could not identify filesystem for \"%s\".", fileUrl))
        );
    }

    /**
     * Determine the number of bytes of a given file. This method is not only a short-cut to
     * {@link FileSystem#getFileSize(String)} but also caches file sizes for performance reasons.
     *
     * @param fileUrl the URL of the file
     * @return the number of bytes of the file if it could be determined
     */
    public static OptionalLong getFileSize(String fileUrl) {
        if (fileSizeCache.containsKey(fileUrl)) {
            return OptionalLong.of(fileSizeCache.get(fileUrl));
        }
        final Optional<FileSystem> fileSystem = FileSystems.getFileSystem(fileUrl);
        if (fileSystem.isPresent()) {
            try {
                final long fileSize = fileSystem.get().getFileSize(fileUrl);
                fileSizeCache.put(fileUrl, fileSize);
                return OptionalLong.of(fileSize);
            } catch (FileNotFoundException e) {
                LOGGER.warn("Could not determine file size.", e);
            }
        }

        return OptionalLong.empty();
    }

    /**
     * Systems such as Spark do not produce a single output file often times. That method tries to detect such
     * split object files to reassemble them correctly. As of now assumes either a Spark layout or a single file.
     *
     * @param ostensibleInputFile the path to that has been written using some framework; might be a dictionary
     * @return all actual input files
     */
    public static Collection<String> findActualInputPaths(String ostensibleInputFile) {
        final Optional<FileSystem> fsOptional = getFileSystem(ostensibleInputFile);

        if (!fsOptional.isPresent()) {
            LoggerFactory.getLogger(FileSystems.class).warn("Could not inspect input file {}.", ostensibleInputFile);
            return Collections.singleton(ostensibleInputFile);
        }

        final FileSystem fs = fsOptional.get();
        if (fs.isDirectory(ostensibleInputFile)) {
            final Collection<String> children = fs.listChildren(ostensibleInputFile);

            // Look for Spark-like directory structure.
            if (children.stream().anyMatch(child -> child.endsWith("_SUCCESS"))) {
                return children.stream().filter(child -> child.matches(".*/part-[m|r|M|R|-]{0,2}\\d+")).collect(Collectors.toList());
            } else {
                throw new RheemException("Could not identify directory structure: " + children);
            }
        }

        return Collections.singleton(ostensibleInputFile);
    }

    /**
     * As {@link #findActualInputPaths(String)} but requires the presence of only a single input file.
     */
    public static String findActualSingleInputPath(String ostensibleInputFile) {
        final Collection<String> inputPaths = FileSystems.findActualInputPaths(ostensibleInputFile);

        if (inputPaths.size() != 1) {
            throw new RheemException(String.format(
                    "Illegal number of files for \"%s\": %s", ostensibleInputFile, inputPaths
            )); // TODO: Add support.
        }

        return inputPaths.iterator().next();
    }

}

package org.qcri.rheem.core.util.fs;

import org.qcri.rheem.core.api.exception.RheemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Tool to work with {@link FileSystem}s.
 */
public class FileSystems {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystems.class);

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
                return children.stream().filter(child -> child.matches(".*/part-\\d{5}")).collect(Collectors.toList());
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
            throw new RheemException("Illegal number of Spark result files: " + inputPaths); // TODO: Add support.
        }

        return inputPaths.iterator().next();
    }

}

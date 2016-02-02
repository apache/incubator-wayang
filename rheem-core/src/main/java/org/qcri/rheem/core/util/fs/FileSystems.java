package org.qcri.rheem.core.util.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

/**
 * Tool to work with {@link FileSystem}s.
 */
public class FileSystems {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystems.class);

    private static Collection<FileSystem> registeredFileSystems = Arrays.asList(new LocalFileSystem());

    private FileSystems() { }
    
    public static Optional<FileSystem> getFileSystem(String fileUrl) {
        return registeredFileSystems.stream()
                .filter(fileSystem -> fileSystem.canHandle(fileUrl))
                .findAny();
    }
    
}

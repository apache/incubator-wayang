package org.qcri.rheem.core.util.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Abstraction for accessing a file system.
 */
public interface FileSystem {

    /**
     * Return the number of bytes of a given file.
     *
     * @param fileUrl URL that identifies the filed
     * @return the number of bytes of the file
     * @throws FileNotFoundException if the file could not be found
     */
    long getFileSize(String fileUrl) throws FileNotFoundException;

    /**
     * @return whether this instance is eligible to operate the file specified in the given {@code url}
     */
    boolean canHandle(String url);

    /**
     * Opens the file specified in the given {@code url}.
     *
     * @param url points to the file to be opened
     * @return an {@link InputStream} with the file's contents
     * @throws IOException if the file cannot be accessed properly for whatever reason
     */
    InputStream open(String url) throws IOException;

}

package org.qcri.rheem.core.util.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;

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

    /**
     * Opens the file specified in the given {@code url} for (over-)writing.
     *
     * @param url points to the file to be created
     * @return an {@link OutputStream} that allows writing to the specified file
     * @throws IOException if the file cannot be created properly for whatever reason
     */
    OutputStream create(String url) throws IOException;

    /**
     * Opens the file specified in the given {@code url} for (over-)writing.
     *
     * @param url points to the file to be created
     * @param forceCreateParentDirs if true, will create parent directories if they don't exist.
     * @return an {@link OutputStream} that allows writing to the specified file
     * @throws IOException if the file cannot be created properly for whatever reason
     */
    OutputStream create(String url, Boolean forceCreateParentDirs) throws IOException;

    boolean isDirectory(String url);

    Collection<String> listChildren(String url);

    /**
     * Deletes the given file at the given {@code url}. To delete directories, specify {@code isRecursiveDelete}.
     *
     * @return whether after the deletion, there is no more file associated with the given {@code url}
     */
    boolean delete(String url, boolean isRecursiveDelete) throws IOException;
}

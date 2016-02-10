package org.qcri.rheem.core.util.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * {@link FileSystem} implementation for the local file system.
 */
public class LocalFileSystem implements FileSystem {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static File toFile(String fileUrl) throws URISyntaxException, MalformedURLException {
        return new File(new URL(fileUrl).toURI());
    }

    @Override
    public long getFileSize(String fileUrl) throws FileNotFoundException {
        try {
            File file = toFile(fileUrl);
            return file.length();
        } catch (MalformedURLException | URISyntaxException e) {
            this.logger.error("Illegal URL: \"{}\"", fileUrl);
            throw new FileNotFoundException("File not found, because the URL is not correct.");
        }
    }

    @Override
    public boolean canHandle(String urlAsString) {
        try {
            URL url = new URL(urlAsString);
            return url.getProtocol().equals("file") &&
                    (url.getHost().equals("") || url.getHost().equals("localhost"));
        } catch (MalformedURLException e) {
            this.logger.error("Illegal URL: \"{}\"", urlAsString);
            return false;
        }
    }

    @Override
    public InputStream open(String url) throws IOException {
        try {
            File file = toFile(url);
            return new FileInputStream(file);
        } catch (URISyntaxException e) {
            throw new IOException("Could not process the given URL.", e);
        }
    }
}

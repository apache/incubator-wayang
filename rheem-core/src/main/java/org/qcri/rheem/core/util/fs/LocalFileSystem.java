package org.qcri.rheem.core.util.fs;

import org.qcri.rheem.core.api.exception.RheemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * {@link FileSystem} implementation for the local file system.
 */
public class LocalFileSystem implements FileSystem {

    private static final Logger logger = LoggerFactory.getLogger(LocalFileSystem.class);

    /**
     * Retrieves a directory that can be used for temporary files.
     *
     * @return a {@link File} representing the directory
     */
    public static File findTempDir() {
        try {
            final File tempFile = File.createTempFile("rheem", "probe");
            tempFile.deleteOnExit();
            return tempFile.getParentFile();
        } catch (IOException e) {
            logger.warn("Could not determine local temp directory.", e);
            return null;
        }
    }

    /**
     * Converts a {@link File} object to a URL.
     *
     * @param file that should be converted
     * @return the {@link String} representation of the URL
     */
    public static String toURL(File file) {
        try {
            return file.toPath().toUri().toURL().toString();
        } catch (MalformedURLException e) {
            throw new RheemException(String.format("Could not create URI for %s", file), e);
        }
    }

    /**
     * Ensure that there is a directory represented by the given {@link File}.
     *
     * @param file the directory that should be ensured
     */
    public static void ensureDir(File file) {
        if (file.exists()) {
            if (!file.isDirectory()) {
                throw new RheemException(String.format("Could not ensure directory %s: It exists, but is not a directory.", file));
            }
        } else if (!file.mkdirs()) {
            throw new RheemException(String.format("Could not ensure directory %s: It does not exist, but could also not be created.", file));
        }
    }

    /**
     * Create an empty file.
     *
     * @param file that should be created
     */
    public static void touch(File file) {
        ensureDir(file.getParentFile());
        try (FileOutputStream fos = new FileOutputStream(file)) {
        } catch (IOException e) {
            throw new RheemException(String.format("Could not create %s.", file), e);
        }
    }

    private static File toFile(String fileUrl) throws URISyntaxException, MalformedURLException {
        if (fileUrl.startsWith("file:")) {
            return new File(new URL(fileUrl).toURI());
        } else {
            logger.warn("Expect URLs, but got {}. Converting it to file:{}...", fileUrl, fileUrl);
            return toFile("file:" + fileUrl);
        }
    }

    @Override
    public long getFileSize(String fileUrl) throws FileNotFoundException {
        try {
            File file = toFile(fileUrl);
            return file.length();
        } catch (MalformedURLException | URISyntaxException e) {
            logger.error("Illegal URL: \"{}\"", fileUrl);
            throw new FileNotFoundException("File not found, because the URL is not correct.");
        }
    }

    @Override
    public boolean canHandle(String urlAsString) {
        if (!urlAsString.startsWith("file:")) return false;
        try {
            URL url = new URL(urlAsString);
            return url.getProtocol().equals("file") &&
                    (url.getHost().equals("") || url.getHost().equals("localhost"));
        } catch (MalformedURLException e) {
            logger.error(String.format("Illegal URL: \"%s\"", urlAsString), e);
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

    @Override
    public OutputStream create(String url) throws IOException {
        return this.create(url, false);
    }

    @Override
    public OutputStream create(String url, Boolean forceCreateParentDirs) throws IOException {
        File file = null;
        try {
            file = toFile(url);
            if (forceCreateParentDirs && file.getParentFile()!=null)
                file.getParentFile().mkdirs();
            return new FileOutputStream(file, false);
        } catch (URISyntaxException e) {
            throw new IOException("Could not process the given URL.", e);
        } catch (IOException e) {
            if (file != null) {
                file.delete();
            }
            throw e;
        }
    }

    @Override
    public boolean isDirectory(String url) {
        try {
            return toFile(url).isDirectory();
        } catch (URISyntaxException | MalformedURLException e) {
            logger.warn("Could not inspect directory.", e);
            return false;
        }
    }

    @Override
    public Collection<String> listChildren(String url) {
        try {
            final File[] files = toFile(url).listFiles();
            if (files == null) {
                return Collections.emptyList();
            }
            return Arrays.stream(files).map(File::toURI).map(Object::toString).collect(Collectors.toList());
        } catch (URISyntaxException | MalformedURLException e) {
            logger.warn("Could not inspect directory.", e);
            return Collections.emptyList();
        }
    }

    @Override
    public boolean delete(String url, boolean isRecursiveDelete) throws IOException {
        try {
            final File file = toFile(url);
            if (!isRecursiveDelete && file.isDirectory()) return false;
            return this.delete(file);
        } catch (URISyntaxException e) {
            throw new IOException("Cannot access file.", e);
        }
    }

    private boolean delete(File file) {
        boolean canDelete = !file.isDirectory() || Arrays.stream(file.listFiles()).allMatch(this::delete);
        return canDelete && file.delete();

    }

}

package org.qcri.rheem.core.util.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.qcri.rheem.core.api.exception.RheemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * {@link FileSystem} immplementation for the HDFS.
 */
public class HadoopFileSystem implements FileSystem {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Tells whether the necessary setup for this {@link FileSystem} has been performed.
     */
    private boolean isInitialized = false;

    /**
     * Make sure that this instance is initialized. This is particularly required to use HDFS {@link URL}s.
     */
    public void ensureInitialized() {
        if (this.isInitialized) return;

        // Add handler for HDFS URL for java.net.URL
        LoggerFactory.getLogger(HadoopFileSystem.class).info("Adding handler for HDFS URLs.");
        try {
            URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        } catch (Throwable t) {
            LoggerFactory.getLogger(HadoopFileSystem.class).error(
                    "Could not set URL stream handler factory.", t
            );
        } finally {
            this.isInitialized = true;
        }
    }

    private org.apache.hadoop.fs.FileSystem getHdfs(String uri) {
        this.ensureInitialized();
        try {
            Configuration conf = new Configuration(true);
            return org.apache.hadoop.fs.FileSystem.get(new URI(uri), conf);
        } catch (IOException | URISyntaxException e) {
            throw new RheemException(String.format("Could not obtain an HDFS client for %s.", uri), e);
        }
    }

    @Override
    public long getFileSize(String fileUrl) throws FileNotFoundException {
        try {
            final FileStatus fileStatus = this.getHdfs(fileUrl).getFileStatus(new Path(fileUrl));
            return fileStatus.getLen();
        } catch (IOException e) {
            throw new FileNotFoundException(String.format("Could not access %s.", fileUrl));
        }
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("hdfs:/");
    }

    @Override
    public InputStream open(String url) throws IOException {
        return this.getHdfs(url).open(new Path(url));
    }

    @Override
    public OutputStream create(String url) throws IOException {
        return this.getHdfs(url).create(new Path(url), true);
    }

    @Override
    public OutputStream create(String url, Boolean forceCreateParentDirs) throws IOException {
        // TODO implement properly.
        return this.create(url);
    }

    @Override
    public boolean isDirectory(String url) {
        try {
            final FileStatus fileStatus = this.getHdfs(url).getFileStatus(new Path(url));
            return fileStatus.isDirectory();
        } catch (IOException e) {
            throw new RheemException(String.format("Could not access %s.", url), e);
        }
    }

    @Override
    public Collection<String> listChildren(String url) {
        try {
            final FileStatus[] fileStatuses = this.getHdfs(url).listStatus(new Path(url));
            return Arrays.stream(fileStatuses)
                    .map(status -> status.getPath().toString())
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RheemException(String.format("Could not access %s.", url), e);
        }
    }

    @Override
    public boolean delete(String url, boolean isRecursiveDelete) throws IOException {
        return this.getHdfs(url).delete(new Path(url), isRecursiveDelete);
    }
}

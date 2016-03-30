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

    static {
        // Add handler for HDFS URL for java.net.URL
        LoggerFactory.getLogger(HadoopFileSystem.class).info("Adding handler for HDFS URLs.");
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private org.apache.hadoop.fs.FileSystem getHdfs(String uri) {
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
        try {
            final URI uri = new URI(url);
            return uri.getScheme().equalsIgnoreCase("hdfs");
        } catch (URISyntaxException e) {
            this.logger.warn(url + " seems to be invalid.", e);
            return false;
        }
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
}

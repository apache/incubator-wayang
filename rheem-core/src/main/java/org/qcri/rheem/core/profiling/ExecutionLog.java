package org.qcri.rheem.core.profiling;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.util.JsonSerializables;
import org.qcri.rheem.core.util.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

/**
 * Stores execution data have been collected by the {@link CrossPlatformExecutor}.
 * The current version uses JSON as serialization format.
 */
public class ExecutionLog implements AutoCloseable {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Path to the repository file.
     */
    private final String repositoryPath;

    /**
     * {@link Configuration} to use.
     */
    private final Configuration configuration;

    /**
     * Created on demand an can be closed as well.
     */
    private BufferedWriter writer;

    private ExecutionLog(Configuration configuration, String repositoryPath) {
        this.configuration = configuration;
        this.repositoryPath = repositoryPath;
        this.logger.info("Curating execution log at {}.", repositoryPath);
    }

    /**
     * Opens an instance according to the {@link Configuration}.
     *
     * @param configuration describes the instance to be opened
     * @return the new instance
     */
    public static ExecutionLog open(Configuration configuration) {
        return open(configuration, configuration.getStringProperty("rheem.core.log.executions"));
    }


    /**
     * Opens an instance.
     *
     * @param configuration  describes the instance to be opened
     * @param repositoryPath location of the instance
     * @return the new instance
     */
    public static ExecutionLog open(Configuration configuration, String repositoryPath) {
        return new ExecutionLog(configuration, repositoryPath);
    }

    /**
     * Stores the given {@link PartialExecution}s in this instance.
     *
     * @param partialExecutions that should be stored
     */
    public void storeAll(Iterable<PartialExecution> partialExecutions) throws IOException {
        final PartialExecution.Serializer serializer = new PartialExecution.Serializer(this.configuration);
        for (PartialExecution partialExecution : partialExecutions) {
            this.store(partialExecution, serializer);
        }
    }

    /**
     * Stores the given {@link PartialExecution} in this instance.
     *
     * @param partialExecution that should be stored
     */
    public void store(PartialExecution partialExecution) throws IOException {
        this.store(partialExecution, new PartialExecution.Serializer(this.configuration));
    }

    /**
     * Stores the given {@link PartialExecution} in this instance.
     *
     * @param partialExecution               that should be stored
     * @param partialExecutionJsonSerializer serializes {@link PartialExecution}s
     */
    private void store(PartialExecution partialExecution, JsonSerializer<PartialExecution> partialExecutionJsonSerializer)
            throws IOException {
        this.write(JsonSerializables.serialize(partialExecution, false, partialExecutionJsonSerializer));
    }

    /**
     * Writes the measuremnt to the {@link #repositoryPath}.
     */
    private void write(JSONObject jsonMeasurement) throws IOException {
        jsonMeasurement.write(this.getWriter());
        writer.write('\n');
    }

    /**
     * Streams the contents of this instance.
     *
     * @return a {@link Stream} of the contained {@link PartialExecution}s
     * @throws IOException
     */
    public Stream<PartialExecution> stream() throws IOException {
        IOUtils.closeQuietly(this.writer);
        this.writer = null;
        final PartialExecution.Serializer serializer = new PartialExecution.Serializer(this.configuration);
        return Files.lines(Paths.get(this.repositoryPath), Charset.forName("UTF-8"))
                .map(line -> {
                    try {
                        return JsonSerializables.deserialize(new JSONObject(line), serializer, PartialExecution.class);
                    } catch (Exception e) {
                        throw new RheemException(String.format("Could not parse \"%s\".", line), e);
                    }
                });
    }

    /**
     * Initializes the {@link #writer} if it does not exist currently.
     *
     * @return the {@link #writer}
     */
    private BufferedWriter getWriter() throws FileNotFoundException, UnsupportedEncodingException {
        if (this.writer != null) {
            return this.writer;
        }

        try {
            File file = new File(this.repositoryPath);
            final File parentFile = file.getParentFile();
            if (!parentFile.exists() && !file.getParentFile().mkdirs()) {
                throw new RheemException("Could not initialize cardinality repository.");
            }
            return this.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), "UTF-8"));
        } catch (RheemException e) {
            throw e;
        } catch (Exception e) {
            throw new RheemException(String.format("Cannot write to %s.", this.repositoryPath), e);
        }
    }

    @Override
    public void close() throws Exception {
        IOUtils.closeQuietly(this.writer);
    }
}

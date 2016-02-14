package org.qcri.rheem.spark.operators;

import org.apache.commons.lang3.Validate;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
 *
 * @see SparkObjectFileSink
 */
public class SparkObjectFileSource<T> extends UnarySource<T> implements SparkExecutionOperator {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final String sourcePath;

    public SparkObjectFileSource(String sourcePath, DataSetType type) {
        super(type, null);
        this.sourcePath = sourcePath;
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        Validate.isTrue(inputRdds.length == 0);
        final JavaRDD<Object> rdd;

        rdd = sparkExecutor.sc.objectFile(this.findCorrectInputPath(this.sourcePath));
        return new JavaRDDLike[] { rdd };
    }


    /**
     * Systems such as Spark do not produce a single output file often times. That method tries to detect such
     * split object files to reassemble them correctly.
     */
    private String findCorrectInputPath(String ostensibleInputFile) {
        final Optional<FileSystem> fileSystem = FileSystems.getFileSystem(ostensibleInputFile);

        if (!fileSystem.isPresent()) {
            LoggerFactory.getLogger(this.getClass()).warn("Could not inspect input file {}.", this.sourcePath);
            return this.sourcePath;

        } else if (fileSystem.get().isDirectory(this.sourcePath)) {
            final Collection<String> children = fileSystem.get().listChildren(this.sourcePath);

            // Look for Spark-like directory structure.
            if (children.stream().anyMatch(child -> child.endsWith("_SUCCESS"))) {
                final List<String> sparkFiles =
                        children.stream().filter(child -> child.matches(".*/part-\\d{5}")).collect(Collectors.toList());
                if (sparkFiles.size() != 1) {
                    throw new RheemException("Illegal number of Spark result files: " + sparkFiles.size());
                }
                LoggerFactory.getLogger(this.getClass()).info("Using input path {} for {}.", sparkFiles.get(0), this);
                return sparkFiles.get(0);
            } else {
                throw new RheemException("Could not identify directory structure: " + children);
            }

        } else {
            return this.sourcePath;
        }
    }


    @Override
    public ExecutionOperator copy() {
        return new SparkObjectFileSource<>(this.sourcePath, this.getType());
    }
}

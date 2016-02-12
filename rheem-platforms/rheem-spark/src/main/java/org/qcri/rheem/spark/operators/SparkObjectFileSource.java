package org.qcri.rheem.spark.operators;

import org.apache.commons.lang3.Validate;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
 *
 * @see SparkObjectFileSink
 */
public class SparkObjectFileSource<T> extends UnarySource<T> implements SparkExecutionOperator {

    private final String sourcePath;

    public SparkObjectFileSource(String sourcePath, DataSetType type) {
        super(type, null);
        this.sourcePath = sourcePath;
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        Validate.isTrue(inputRdds.length == 0);
        final JavaRDD<Object> rdd = sparkExecutor.sc.objectFile(this.sourcePath);
        return new JavaRDDLike[] { rdd };
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkObjectFileSource<>(this.sourcePath, this.getType());
    }
}

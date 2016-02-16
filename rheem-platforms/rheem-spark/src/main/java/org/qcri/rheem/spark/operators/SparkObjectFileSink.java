package org.qcri.rheem.spark.operators;

import org.apache.commons.lang3.Validate;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
 *
 * @see SparkObjectFileSource
 */
public class SparkObjectFileSink<T> extends UnarySink<T> implements SparkExecutionOperator {

    private final String targetPath;

    public SparkObjectFileSink(String targetPath, DataSetType type) {
        super(type, null);
        this.targetPath = targetPath;
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        Validate.isTrue(inputRdds.length == 1);
        JavaRDD<T> rdd = (JavaRDD<T>) inputRdds[0];
        rdd.saveAsObjectFile(this.targetPath);
        return new JavaRDDLike[0];
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkObjectFileSink<>(this.targetPath, this.getType());
    }

    public String getTargetPath() {
        return this.targetPath;
    }
}

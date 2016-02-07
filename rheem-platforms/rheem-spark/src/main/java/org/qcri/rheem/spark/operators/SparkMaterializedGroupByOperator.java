package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.operators.MaterializedGroupByOperator;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;


/**
 * Spark implementation of the {@link ReduceByOperator}.
 */
public class SparkMaterializedGroupByOperator<Type, KeyType>
        extends MaterializedGroupByOperator<Type, KeyType>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type          type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param keyDescriptor describes how to extract the key from data units
     */
    public SparkMaterializedGroupByOperator(DataSetType<Type> type, TransformationDescriptor<Type, KeyType> keyDescriptor) {
        super(type, keyDescriptor);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final JavaRDD<Type> inputStream = (JavaRDD<Type>) inputStreams[0];
        final JavaPairRDD<KeyType, Iterable<Type>> outputStream = inputStream.groupBy(compiler.compile(this.keyDescriptor));

        return new JavaRDDLike[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkMaterializedGroupByOperator<>(getType(), getKeyDescriptor());
    }
}

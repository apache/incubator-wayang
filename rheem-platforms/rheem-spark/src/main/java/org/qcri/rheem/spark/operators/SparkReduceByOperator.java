package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Spark implementation of the {@link ReduceByOperator}.
 */
public class SparkReduceByOperator<Type, KeyType>
        extends ReduceByOperator<Type, KeyType>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     *
     * @param type        type of the reduce elements (i.e., type of {@link #getInput()} and {@link #getOutput()})
     * @param keyDescriptor    describes how to extract the key from data units
     * @param reduceDescriptor describes the reduction to be performed on the elements
     */
    public SparkReduceByOperator(DataSetType<Type> type, TransformationDescriptor<Type, KeyType> keyDescriptor,
                                ReduceDescriptor<Type> reduceDescriptor) {
        super(type, keyDescriptor, reduceDescriptor);
    }

    @Override
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final JavaRDD<Type> inputStream = (JavaRDD<Type>) inputStreams[0];
        //final Function<Type, KeyType> keyExtractor = compiler.compile(this.keyDescriptor);
        //final BinaryOperator<Type> reduceFunction = compiler.compile(this.reduceDescriptor);
        //final JavaPairRDD<KeyType, Type> outputStream = inputStream.mapToPair(null).reduceByKey(reduceFunction);

        return new JavaRDDLike[]{null};
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkReduceByOperator<>(getType(), getKeyDescriptor(), getReduceDescriptor());
    }
}

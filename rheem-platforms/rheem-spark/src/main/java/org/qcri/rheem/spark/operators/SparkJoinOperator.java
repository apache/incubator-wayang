package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.PairFunction;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.function.KeyExtractorDescriptor;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import scala.Tuple2;

/**
 * Spark implementation of the {@link JoinOperator}.
 */
public class SparkJoinOperator<InputType0, InputType1, KeyType>
        extends JoinOperator<InputType0, InputType1, KeyType>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     *
     */
    public SparkJoinOperator(DataSetType <InputType0> inputType0, DataSetType inputType1,
                            KeyExtractorDescriptor<InputType0, KeyType> keyDescriptor0,
                            KeyExtractorDescriptor<InputType1, KeyType> keyDescriptor1) {

        super(inputType0, inputType1, keyDescriptor0, keyDescriptor1);
    }

    @Override
    public JavaRDDLike[] evaluate (JavaRDDLike[] inputStreams, FunctionCompiler compiler) {
        if (inputStreams.length != 2) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final JavaRDD<InputType0> inputStream0 = (JavaRDD<InputType0>) inputStreams[0];
        final JavaRDD<InputType1> inputStream1 = (JavaRDD<InputType1>) inputStreams[1];

        final PairFunction<InputType0, KeyType, InputType0> keyExtractor0 = compiler.compile(this.keyDescriptor0);
        final PairFunction<InputType1, KeyType, InputType1> keyExtractor1 = compiler.compile(this.keyDescriptor1);
        JavaPairRDD<KeyType, InputType0> pairStream0 = inputStream0.mapToPair(keyExtractor0);
        JavaPairRDD<KeyType, InputType1> pairStream1 = inputStream1.mapToPair(keyExtractor1);

        final JavaPairRDD<KeyType, Tuple2<InputType0, InputType1>> outputPair = pairStream0.join(pairStream1);

        // convert from scala tuple to rheem tuple
        final JavaRDD<org.qcri.rheem.basic.data.Tuple2> outputStream = outputPair
                .map((input)->new org.qcri.rheem.basic.data.Tuple2(input._2._1, input._2._2));

        return new JavaRDDLike[]{outputStream};
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkJoinOperator<>(getInputType0(), getInputType1(),
                getKeyDescriptor0(), getKeyDescriptor1());
    }
}

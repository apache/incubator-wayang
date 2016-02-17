package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;

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
                            TransformationDescriptor<InputType0, KeyType> keyDescriptor0,
                             TransformationDescriptor<InputType1, KeyType> keyDescriptor1) {

        super(inputType0, inputType1, keyDescriptor0, keyDescriptor1);
    }

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final JavaRDD<InputType0> inputRdd0 = inputs[0].provideRdd();
        final JavaRDD<InputType1> inputRdd1 = inputs[1].provideRdd();

        final PairFunction<InputType0, KeyType, InputType0> keyExtractor0 = compiler.compileToKeyExtractor(this.keyDescriptor0);
        final PairFunction<InputType1, KeyType, InputType1> keyExtractor1 = compiler.compileToKeyExtractor(this.keyDescriptor1);
        JavaPairRDD<KeyType, InputType0> pairStream0 = inputRdd0.mapToPair(keyExtractor0);
        JavaPairRDD<KeyType, InputType1> pairStream1 = inputRdd1.mapToPair(keyExtractor1);

        final JavaPairRDD<KeyType, scala.Tuple2<InputType0, InputType1>> outputPair = pairStream0.join(pairStream1);

        // convert from scala tuple to rheem tuple
        final JavaRDD<Tuple2<InputType0, InputType1>> outputRdd = outputPair
                .map(new TupleConverter<>());

        outputs[0].acceptRdd(outputRdd);
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkJoinOperator<>(this.getInputType0(), this.getInputType1(),
                this.getKeyDescriptor0(), this.getKeyDescriptor1());
    }

    /**
     * Migrates {@link scala.Tuple2} to {@link Tuple2}.
     * <p><i>TODO: See, if we can somehow dodge all this conversion, which is likely to happen a lot.</i></p>
     */
    private static class TupleConverter<InputType0, InputType1, KeyType>
            implements Function<scala.Tuple2<KeyType, scala.Tuple2<InputType0, InputType1>>, Tuple2<InputType0, InputType1>> {

        @Override
        public Tuple2<InputType0, InputType1> call(scala.Tuple2<KeyType, scala.Tuple2<InputType0, InputType1>> scalaTuple) throws Exception {
            return new Tuple2<>(scalaTuple._2._1, scalaTuple._2._2);
        }
    }
}

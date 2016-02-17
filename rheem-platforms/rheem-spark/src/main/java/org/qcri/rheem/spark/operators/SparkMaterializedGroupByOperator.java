package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.operators.MaterializedGroupByOperator;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import scala.Tuple2;


/**
 * Spark implementation of the {@link MaterializedGroupByOperator}.
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
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final JavaRDD<Type> inputRdd = inputs[0].provideRdd();
        final Function<Type, KeyType> keyExtractor = compiler.compile(this.keyDescriptor, this, inputs);
        final Function<scala.Tuple2<KeyType, Iterable<Type>>, Iterable<Type>> projector = new GroupProjector<>();
        final JavaRDD<Iterable<Type>> outputRdd = inputRdd
                .groupBy(keyExtractor)
                .map(projector);

        // TODO: MaterializedGroupByOperator actually prescribes to return Iterators, not Iterables.
        outputs[0].acceptRdd(outputRdd);
    }

    @Override
    public ExecutionOperator copy() {
        return new SparkMaterializedGroupByOperator<>(this.getType(), this.getKeyDescriptor());
    }

    private static class GroupProjector<Key, Type> implements Function<scala.Tuple2<Key, Iterable<Type>>, Iterable<Type>> {

        @Override
        public Iterable<Type> call(Tuple2<Key, Iterable<Type>> groupWithKey) throws Exception {
            return groupWithKey._2;
        }

    }

}

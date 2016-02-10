package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.basic.operators.MaterializedGroupByOperator;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
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
    public JavaRDDLike[] evaluate(JavaRDDLike[] inputRdds, FunctionCompiler compiler) {
        if (inputRdds.length != 1) {
            throw new IllegalArgumentException("Cannot evaluate: Illegal number of input streams.");
        }

        final JavaRDD<Type> inputRdd = (JavaRDD<Type>) inputRdds[0];
        final Function<Type, KeyType> keyExtractor = compiler.compile(this.keyDescriptor);
        final Function<scala.Tuple2<KeyType, Iterable<Type>>, Iterable<Type>> projector = new GroupProjector<>();
        final JavaRDDLike outputRdd = inputRdd
                .groupBy(keyExtractor)
                .map(projector);

        // TODO: MaterializedGroupByOperator actually prescribes to return Iterators, not Iterables.
        return new JavaRDDLike[]{outputRdd};
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

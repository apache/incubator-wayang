package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Spark implementation of the {@link JoinOperator}.
 */
public class SparkJoinOperator<InputType0, InputType1, KeyType>
        extends JoinOperator<InputType0, InputType1, KeyType>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public SparkJoinOperator(DataSetType<InputType0> inputType0,
                             DataSetType<InputType1> inputType1,
                             TransformationDescriptor<InputType0, KeyType> keyDescriptor0,
                             TransformationDescriptor<InputType1, KeyType> keyDescriptor1) {

        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkJoinOperator(JoinOperator<InputType0, InputType1, KeyType> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input0 = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance input1 = (RddChannel.Instance) inputs[1];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final JavaRDD<InputType0> inputRdd0 = input0.provideRdd();
        final JavaRDD<InputType1> inputRdd1 = input1.provideRdd();

        FunctionCompiler compiler = sparkExecutor.getCompiler();
        final PairFunction<InputType0, KeyType, InputType0> keyExtractor0 = compiler.compileToKeyExtractor(this.keyDescriptor0);
        final PairFunction<InputType1, KeyType, InputType1> keyExtractor1 = compiler.compileToKeyExtractor(this.keyDescriptor1);
        JavaPairRDD<KeyType, InputType0> pairStream0 = inputRdd0.mapToPair(keyExtractor0);
        JavaPairRDD<KeyType, InputType1> pairStream1 = inputRdd1.mapToPair(keyExtractor1);

        final JavaPairRDD<KeyType, scala.Tuple2<InputType0, InputType1>> outputPair =
                pairStream0.<InputType1>join(pairStream1, sparkExecutor.getNumDefaultPartitions());
        this.name(outputPair);

        // convert from scala tuple to rheem tuple
        final JavaRDD<Tuple2<InputType0, InputType1>> outputRdd = outputPair
                .map(new TupleConverter<>());
        this.name(outputRdd);

        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
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

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.join.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                SparkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor0, configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.keyDescriptor1, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

}

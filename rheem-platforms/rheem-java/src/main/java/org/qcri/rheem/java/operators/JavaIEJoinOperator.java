package org.qcri.rheem.java.operators;

import org.qcri.rheem.basic.data.Data;
import org.qcri.rheem.basic.data.JoinCondition;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.IEJoinOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.subOperator.BitSetJoin;
import org.qcri.rheem.java.operators.subOperator.DataComparator;
import org.qcri.rheem.java.operators.subOperator.extractData;
import scala.Tuple2;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link IEJoinOperator}.
 */
public class JavaIEJoinOperator<Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>,Input>
        extends IEJoinOperator<Type0, Type1,Input>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     */
    public JavaIEJoinOperator(DataSetType<Input> inputType0, DataSetType<Input> inputType1,
                              TransformationDescriptor<Input,Type0> get0Pivot, TransformationDescriptor<Input,Type0> get1Pivot, JoinCondition cond0,
                              TransformationDescriptor<Input,Type1> get0Ref,  TransformationDescriptor<Input,Type1> get1Ref, JoinCondition cond1) {
        super(inputType0, inputType1, get0Pivot, get1Pivot, cond0, get0Ref, get1Ref, cond1);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler) {
        StreamChannel.Instance outputChannel = (StreamChannel.Instance) outputs[0];

        Stream<Input> stream0;
        Stream<Input> stream1;
        if (inputs[0] instanceof CollectionChannel.Instance) {
            final Collection<Input> collection = ((CollectionChannel.Instance) inputs[0]).provideCollection();
            stream0 = collection.stream();
            stream1 = ((JavaChannelInstance) inputs[1]).provideStream();
        } else if (inputs[1] instanceof CollectionChannel.Instance) {
            stream0 = ((JavaChannelInstance) inputs[0]).provideStream();
            final Collection<Input> collection = ((CollectionChannel.Instance) inputs[1]).provideCollection();
            stream1 = collection.stream();
        } else {
            // Fallback: Materialize one side.
            final Collection<Input> collection = ((JavaChannelInstance) inputs[0]).<Input>provideStream().collect(Collectors.toList());
            stream0 = collection.stream();
            stream1 = ((JavaChannelInstance) inputs[1]).provideStream();
        }

        final Function<Input, Type0> get0Pivot_ = compiler.compile(this.get0Pivot);
        final Function<Input, Type0> get1Pivot_ = compiler.compile(this.get1Pivot);
        final Function<Input, Type1> get0Ref_ = compiler.compile(this.get0Ref);
        final Function<Input, Type1> get1Ref_ = compiler.compile(this.get1Ref);

        Object[] stream0R = stream0.toArray();
        Object[] stream1R = stream1.toArray();

        ArrayList<Tuple2<Data<Type0, Type1>, Input>> list0 = new ArrayList<Tuple2<Data<Type0, Type1>, Input>>();
        ArrayList<Tuple2<Data<Type0, Type1>, Input>> list1 = new ArrayList<Tuple2<Data<Type0, Type1>, Input>>();

        for (int i = 0; i < stream0R.length; i++) {
            list0.add(new Tuple2<Data<Type0, Type1>, Input>(new extractData<Type0, Type1,Input>(get0Pivot_, get0Ref_).call((Input) stream0R[i]), (Input) stream0R[i]));
        }
        for (int i = 0; i < stream1R.length; i++) {
            list1.add(new Tuple2<Data<Type0, Type1>, Input>(new extractData<Type0, Type1,Input>(get1Pivot_, get1Ref_).call((Input) stream1R[i]), (Input) stream1R[i]));
        }

        Collections.sort(list0, new DataComparator<Type0, Type1, Input>(list1ASC, list1ASCSec));
        Collections.sort(list1, new DataComparator<Type0, Type1, Input>(list2ASC, list2ASCSec));

        long partCount = list0.size();
        long partCount2 = list1.size();

        // count starting point for rdd2
        long cnt2 = partCount + 1;

        // Give unique ID for rdd1 & rdd2
        for (int i = 0; i < partCount; i++) {
            list0.get(i)._1().setRowID(i);
        }
        for (int i = 0; i < partCount2; i++) {
            list1.get(i)._1().setRowID(i + cnt2);
        }

        ArrayList<Tuple2<Input, Input>> result = new BitSetJoin<Type0, Type1,Input>(list1ASC, list2ASC,
                list1ASCSec, list2ASCSec, equalReverse, false, cond0).call(list0, list1);

        outputChannel.<Tuple2<Input, Input>>accept(result.stream());
    }

    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(Configuration configuration) {
        return Optional.of(new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(2, 1, 0.9d, (inCards, outCards) -> outCards[0] * 21 + 900000),
                LoadEstimator.createFallback(2, 1)
        ));
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaIEJoinOperator<Type0, Type1,Input>(this.getInputType0(), this.getInputType1(),
                get0Pivot, get1Pivot, cond0, get0Ref, get1Ref, cond1);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}

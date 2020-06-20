package org.qcri.rheem.iejoin.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Copyable;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.iejoin.data.Data;
import org.qcri.rheem.iejoin.operators.spark_helpers.BitSetJoin;
import org.qcri.rheem.iejoin.operators.spark_helpers.List2AttributesObjectSkinny;
import org.qcri.rheem.iejoin.operators.spark_helpers.addUniqueID;
import org.qcri.rheem.iejoin.operators.spark_helpers.build2ListObject;
import org.qcri.rheem.iejoin.operators.spark_helpers.extractData;
import org.qcri.rheem.iejoin.operators.spark_helpers.filterUnwantedBlocks;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;
import scala.Tuple2;
import scala.Tuple5;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Spark implementation of the {@link   IESelfJoinOperator}.
 */
public class SparkIESelfJoinOperator<Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>, Input extends Copyable<Input>>
        extends IESelfJoinOperator<Type0, Type1, Input>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public SparkIESelfJoinOperator(DataSetType<Input> inputType,
                                   TransformationDescriptor<Input, Type0> get0Pivot, IEJoinMasterOperator.JoinCondition cond0, TransformationDescriptor<Input, Type1> get0Ref, IEJoinMasterOperator.JoinCondition cond1) {
        super(inputType, get0Pivot, cond0, get0Ref, cond1);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();


        final RddChannel.Instance rinput = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];


        final JavaRDD<Input> rdd0 = rinput.provideRdd();
        JavaPairRDD<List2AttributesObjectSkinny<Type0, Type1>, List2AttributesObjectSkinny<Type0, Type1>> listOfListObject = null;
        JavaPairRDD<Long, Tuple2<Long, Input>> r1RowIDS = null;
        JavaPairRDD<Long, Tuple2<Long, Input>> r2RowIDS = null;
        JavaRDD<org.qcri.rheem.basic.data.Tuple2<Input, Input>> outRDD = null;

        //get this from the user (SOMEHOW)
        /*ArrayList<String> attSymbols = new ArrayList<String>();
        attSymbols.add("<");//prim
        attSymbols.add(">");//ref
        boolean list1ASC = false;
        boolean list1ASCSec = false;
        boolean list2ASC = false;
        boolean list2ASCSec = false;
        boolean equalReverse = false;*/

        final Function<Input, Type0> get0Pivot_ = sparkExecutor.getCompiler().compile(this.get0Pivot, this, operatorContext, inputs);
        final Function<Input, Type1> get0Ref_ = sparkExecutor.getCompiler().compile(this.get0Ref, this, operatorContext, inputs);
        // count larger partition size in rdd1 & rdd2
        int partCount = rdd0.mapPartitions(
                input -> {
                    Iterator<Input> it = input;
                    ArrayList<Integer> out = new ArrayList<Integer>(1);
                    int i = 0;
                    while (it.hasNext()) {
                        it.next();
                        i++;
                    }
                    out.add(i);
                    return out.iterator();
                }
                , true).reduce((input1, input2) -> Math.max(input1, input2));

        // Get unique ID for rdd1 & rdd2
        JavaRDD<Tuple2<Long, Input>> inputRDD1UID = rdd0
                .mapPartitionsWithIndex(new addUniqueID(partCount, 0), true);

        // extract pivot attribute and sort
        JavaPairRDD<Data<Type0, Type1>, Tuple2<Long, Input>> keyedDataRDD1 = inputRDD1UID
                .keyBy(new extractData<Type0, Type1, Input>(get0Pivot_, get0Ref_)).sortByKey(new Data.Comparator<Type0, Type1>(list1ASC, list1ASCSec));

        // convert each partition to List2AttributesObjectSkinny
        JavaRDD<List2AttributesObjectSkinny<Type0, Type1>> listObjectDataRDD1 = keyedDataRDD1
                .values().mapPartitionsWithIndex(
                        new build2ListObject<Type0, Type1, Input>(list1ASC, list1ASCSec, get0Pivot_, get0Ref_), true);

        // get partition ID for each List2AttributesObjectSkinny object
        JavaPairRDD<Long, List2AttributesObjectSkinny<Type0, Type1>> listObjectDataRDD1Indexd = listObjectDataRDD1
                .keyBy(input -> input.getPartitionID());

        // get information on each List2AttributesObjectSkinny object
        JavaRDD<Tuple5<Long, Type0, Type0, Type1, Type1>> rdd1TinyObjects = listObjectDataRDD1
                .map(in -> {
                    Tuple2<Type1, Type1> refMinMax = in.findMinMaxRank();
                    return new Tuple5<Long, Type0, Type0, Type1, Type1>(in.getPartitionID(),
                            in.getHeadTupleValue(), in.getTailTupleData().getValue(),
                            refMinMax._1(), refMinMax._2());
                }).repartition(1);

        // cartesian all block information, filter unwanted block pairs
        JavaPairRDD<Long, Long> myBlocks = rdd1TinyObjects
                .cartesian(rdd1TinyObjects)
                .filter(new filterUnwantedBlocks<Type0, Type1>(this.cond0,
                        list2ASC)).mapToPair(in -> new Tuple2<Long, Long>(in._1()._1(), in._2()._1()));

        listOfListObject = myBlocks.join(listObjectDataRDD1Indexd)
                .mapToPair(in -> new Tuple2<Long, List2AttributesObjectSkinny<Type0, Type1>>(in._2()._1(), in._2()._2())).join(listObjectDataRDD1Indexd)
                .values().mapToPair(in -> in);

        // Get row IDs in RDDs to be joined later
        r1RowIDS = inputRDD1UID.keyBy(in -> in._1());
        r2RowIDS = r1RowIDS;

        JavaPairRDD<Long, Long> tmpOut1 = null;

        tmpOut1 = listOfListObject.flatMapToPair(new BitSetJoin<Type0, Type1>(list1ASC,
                list2ASC, list1ASCSec, list2ASCSec, equalReverse,
                true,
                this.cond0));

        JavaPairRDD<Long, Input> tmpOut2 = tmpOut1.join(r1RowIDS).mapToPair(
                in -> new Tuple2<Long, Input>(in._2()._1(), in._2()._2()._2()));

        outRDD = tmpOut2.join(r2RowIDS).map(in -> new org.qcri.rheem.basic.data.Tuple2<Input, Input>(in._2()._1(), in._2()._2()._2()));

        output.accept(outRDD, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkIESelfJoinOperator<Type0, Type1, Input>(this.getInputType(), this.get0Pivot, cond0, this.get0Ref, cond1);
    }

    //TODO:
    //A copy from SparkCartesianOperator.. Need checking
    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    //TODO:
    //A copy from SparkCartesianOperator.. Need checking
    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        // TODO: Check if RDD.partition() is an action.
        return false;
    }

}


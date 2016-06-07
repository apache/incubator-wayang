package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.data.Data;
import org.qcri.rheem.basic.data.JoinCondition;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.operators.IEJoinOperator;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.operators.subOperators.*;
import org.qcri.rheem.spark.platform.SparkExecutor;
import scala.Tuple2;
import scala.Tuple5;

import java.util.*;

/**
 * Spark implementation of the {@link   IEJoinOperator}.
 */
public class SparkIEJoinOperator<Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>>
        extends IEJoinOperator<Type0, Type1>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     */
    public SparkIEJoinOperator(DataSetType<Record> inputType0, DataSetType<Record> inputType1,
                               int get0Pivot, int get1Pivot, JoinCondition cond0,
                               int get0Ref, int get1Ref, JoinCondition cond1) {
        super(inputType0, inputType1, get0Pivot, get1Pivot, cond0, get0Ref, get1Ref, cond1);
    }

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();


        final RddChannel.Instance rinput0 = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance rinput1 = (RddChannel.Instance) inputs[1];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];


        final JavaRDD<Record> rdd0 = rinput0.provideRdd();
        final JavaRDD<Record> rdd1 = rinput1.provideRdd();

        JavaPairRDD<List2AttributesObjectSkinny<Type0, Type1>, List2AttributesObjectSkinny<Type0, Type1>> listOfListObject = null;
        JavaPairRDD<Long, Tuple2<Long, Record>> r1RowIDS = null;
        JavaPairRDD<Long, Tuple2<Long, Record>> r2RowIDS = null;
        JavaRDD<org.qcri.rheem.basic.data.Tuple2<Record, Record>> outRDD = null;

        //get this from the user (SOMEHOW)
        //ArrayList<String> attSymbols = new ArrayList<String>();
        //attSymbols.add("<");//prim
        //attSymbols.add(">");//ref
        //int primPivotIndex = 0; //exInput.get(0)._1()
        //int secPivotIndex = 0; //exInput.get(0)._2()
        //int primRefIndex = 1; //exInput.get(1)._1()
        //int secRefIndex = 1; //exInput.get(1)._2()
        // boolean list1ASC = false;
        //boolean list1ASCSec = false;
        //boolean list2ASC = false;
        //boolean list2ASCSec = false;
        //boolean equalReverse = false;


        // count larger partition size in rdd1 & rdd2
        int partCount = rdd0.mapPartitions(input -> {
                    Iterator<Record> it = input;
                    ArrayList<Integer> out = new ArrayList<Integer>(1);
                    int i = 0;
                    while (it.hasNext()) {
                        it.next();
                        i++;
                    }
                    out.add(i);
                    return out;
                }
                , true).reduce((input1, input2) -> Math.max(input1, input2));

        int partCount2 = rdd1.mapPartitions(input -> {
                    Iterator<Record> it = input;
                    ArrayList<Integer> out = new ArrayList<Integer>(1);
                    int i = 0;
                    while (it.hasNext()) {
                        it.next();
                        i++;
                    }
                    out.add(i);
                    return out;
                }
                , true).reduce((input1, input2) -> Math.max(input1, input2));

        // count starting point for rdd2
        int cnt2 = partCount * rdd0.rdd().partitions().length;

        // Get unique ID for rdd1 & rdd2
        JavaRDD<Tuple2<Long, Record>> inputRDD1UID = rdd0
                .mapPartitionsWithIndex(new addUniqueID(partCount, 0), true);
        JavaRDD<Tuple2<Long, Record>> inputRDD2UID = rdd1
                .mapPartitionsWithIndex(new addUniqueID(partCount2, cnt2),
                        true);

        // extract pivot attribute and sort
        JavaPairRDD<Data<Type0, Type1>, Tuple2<Long, Record>> keyedDataRDD1 = inputRDD1UID
                .keyBy(new extractData<Type0, Type1>(get0Pivot, get0Ref)).sortByKey(new DataComparator<Type0, Type1>(list1ASC, list1ASCSec));
        JavaPairRDD<Data<Type0, Type1>, Tuple2<Long, Record>> keyedDataRDD2 = inputRDD2UID
                .keyBy(new extractData<Type0, Type1>(get1Pivot, get1Ref)).sortByKey(new DataComparator<Type0, Type1>(list2ASC, list2ASCSec));

        // convert each partition to List2AttributesObjectSkinny
        JavaRDD<List2AttributesObjectSkinny<Type0, Type1>> listObjectDataRDD1 = keyedDataRDD1
                .values().mapPartitionsWithIndex(
                        new build2ListObject<Type0, Type1>(list1ASC, list1ASCSec, get0Pivot, get0Ref), true);

        JavaRDD<List2AttributesObjectSkinny<Type0, Type1>> listObjectDataRDD2 = keyedDataRDD2
                .values().mapPartitionsWithIndex(
                        new build2ListObject<Type0, Type1>(list2ASC, list2ASCSec, get1Pivot, get1Ref), true);

        // get partition ID for each List2AttributesObjectSkinny object
        JavaPairRDD<Long, List2AttributesObjectSkinny<Type0, Type1>> listObjectDataRDD1Indexd = listObjectDataRDD1
                .keyBy(in -> in.getPartitionID());
        JavaPairRDD<Long, List2AttributesObjectSkinny<Type0, Type1>> listObjectDataRDD2Indexd = listObjectDataRDD2
                .keyBy(in -> in.getPartitionID());

        // get information on each List2AttributesObjectSkinny object
        JavaRDD<Tuple5<Long, Type0, Type0, Type1, Type1>> rdd1TinyObjects = listObjectDataRDD1
                .map(in -> {
                    Tuple2<Type1, Type1> refMinMax = in.findMinMaxRank();
                    return new Tuple5<Long, Type0, Type0, Type1, Type1>(in.getPartitionID(),
                            in.getHeadTupleValue(), in.getTailTupleData().getValue(),
                            refMinMax._1(), refMinMax._2());
                }).repartition(1);
        JavaRDD<Tuple5<Long, Type0, Type0, Type1, Type1>> rdd2TinyObjects = listObjectDataRDD2
                .map(in -> {
                    Tuple2<Type1, Type1> refMinMax = in.findMinMaxRank();
                    return new Tuple5<Long, Type0, Type0, Type1, Type1>(in.getPartitionID(),
                            in.getHeadTupleValue(), in.getTailTupleData().getValue(),
                            refMinMax._1(), refMinMax._2());
                }).repartition(1);

        // cartesian all block information, filter unwanted block pairs
        JavaPairRDD<Long, Long> myBlocks = rdd1TinyObjects
                .cartesian(rdd2TinyObjects)
                .filter(new filterUnwantedBlocks<Type0, Type1>(cond0,
                        list2ASC)).mapToPair(in -> new Tuple2<Long, Long>(in._1()._1(), in._2()._1()));

        listOfListObject = myBlocks.join(listObjectDataRDD1Indexd)
                .mapToPair(in -> new Tuple2<Long, List2AttributesObjectSkinny<Type0, Type1>>(in._2()._1(), in._2()._2())).join(listObjectDataRDD2Indexd)
                .values().mapToPair(in -> in);

        // Get row IDs in RDDs to be joined later
        r1RowIDS = inputRDD1UID.keyBy(in -> in._1());
        r2RowIDS = inputRDD2UID.keyBy(in -> in._1());
        //}

        JavaPairRDD<Long, Long> tmpOut1 = null;

        tmpOut1 = listOfListObject.flatMapToPair(new BitSetJoin<Type0, Type1>(list1ASC,
                list2ASC, list1ASCSec, list2ASCSec, equalReverse,
                false,
                cond0));

        JavaPairRDD<Long, Record> tmpOut2 = tmpOut1.join(r1RowIDS).mapToPair(
                in -> new Tuple2<Long, Record>(in._2()._1(), in._2()._2()._2()));

        outRDD = tmpOut2.join(r2RowIDS).map(in -> new org.qcri.rheem.basic.data.Tuple2<Record, Record>(in._2()._1(), in._2()._2()._2()));

        output.accept(outRDD, sparkExecutor);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkIEJoinOperator<Type0, Type1>(this.getInputType0(), this.getInputType1(),
                get0Pivot, get1Pivot, cond0, get0Ref, get1Ref, cond1);
    }

    //TODO:
    //A copy from SparkCartesianOperator.. Need checking
    @Override
    public Optional<LoadProfileEstimator> getLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(2, 1, .9d, (inputCards, outputCards) -> 20000000 * inputCards[0] + 10000000 * inputCards[1] + 100 * outputCards[0] + 5500000000L),
                new DefaultLoadEstimator(2, 1, .9d, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(2, 1, .9d, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(2, 1, .9d, (inputCards, outputCards) -> 20000 * (inputCards[0] + inputCards[1]) + 1700000),
                0.1d,
                1000
        );

        return Optional.of(mainEstimator);
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
}

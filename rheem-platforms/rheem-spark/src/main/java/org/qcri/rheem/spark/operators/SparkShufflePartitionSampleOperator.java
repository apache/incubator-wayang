package org.qcri.rheem.spark.operators;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;
import scala.collection.JavaConversions;
import scala.collection.convert.Wrappers;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;


/**
 * Spark implementation of the {@link SparkShufflePartitionSampleOperator}.
 */
public class SparkShufflePartitionSampleOperator<Type>
        extends SampleOperator<Type>
        implements SparkExecutionOperator {

    private Random rand;

    private int partitionID = 0;
    private int tupleID = 0;
    private int nb_partitions = 0;
    private List<Integer> partitions;
    private JavaRDD<Type> shuffledRDD;

    /**
     * Creates a new instance.
     */
    public SparkShufflePartitionSampleOperator(IntUnaryOperator sampleSizeFunction, DataSetType<Type> type, LongUnaryOperator seedFunction) {
        super(sampleSizeFunction, type, Methods.SHUFFLE_PARTITION_FIRST, seedFunction);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkShufflePartitionSampleOperator(SampleOperator<Type> that) {
        super(that);
        assert that.getSampleMethod() == Methods.SHUFFLE_PARTITION_FIRST || that.getSampleMethod() == Methods.ANY;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        RddChannel.Instance input = (RddChannel.Instance) inputs[0];

        JavaRDD<Type> inputRdd = input.provideRdd();
        long datasetSize = this.isDataSetSizeKnown() ?
                this.getDatasetSize() :
                inputRdd.cache().count();
        int sampleSize = this.getSampleSize(operatorContext);
        if (sampleSize >= datasetSize) { //return all and return
            ((CollectionChannel.Instance) outputs[0]).accept(inputRdd.collect());
            return null;
        }
        long seed = this.getSeed(operatorContext);
        rand = new Random(seed);

        List<Type> result = new ArrayList<>();
        final SparkContext sparkContext = inputRdd.context();

        boolean miscalculated = false;
        do {
            if (tupleID == 0) {
                if (nb_partitions == 0) { //it's the first time we sample or we read all partitions already, start again
                    nb_partitions = inputRdd.partitions().size();
                    partitions = new ArrayList<>();
                    for (int i = 0; i < nb_partitions; i++)
                        partitions.add(i, i);
                }
                //choose a random partition
                partitionID = partitions.remove(rand.nextInt(nb_partitions--));
                // shuffle the partition
                shuffledRDD = inputRdd.<Type>mapPartitionsWithIndex(new ShufflePartition<>(partitionID, seed), true).cache();
                miscalculated = false;
            }
            List<Integer> pars = new ArrayList<>(1);
            pars.add(partitionID);
            //read sequentially from partitionID
            Object samples = sparkContext.runJob(shuffledRDD.rdd(),
                    new TakeSampleFunction(tupleID, tupleID + sampleSize),
                    (scala.collection.Seq) JavaConversions.asScalaBuffer(pars), scala.reflect.ClassTag$.MODULE$.apply(List.class));

            tupleID += sampleSize;
            result.addAll(((List<Type>[]) samples)[0]);
            if (result.size() < sampleSize) { //we reached end of partition, start again
                miscalculated = true;
                tupleID = 0;
                sampleSize = sampleSize - result.size();
            }
        } while (miscalculated);

        // assuming the sample is small better use a collection instance, the optimizer can transform the output if necessary
        ((CollectionChannel.Instance) outputs[0]).accept(result);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkShufflePartitionSampleOperator<>(this);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Collections.singleton("rheem.spark.shuffle-partition-sample.load");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return true;
    }

}

class ShufflePartition<V, T, R> implements Function2<V, T, R> {

    private int partitionID;
    private Random rand;

    ShufflePartition(int partitionID) {
        this.partitionID = partitionID;
        this.rand = new Random();
    }

    ShufflePartition(int partitionID, long seed) {
        this.partitionID = partitionID;
        this.rand = new Random(seed);

    }

    @Override
    public Object call(Object o, Object o2) throws Exception {
        int myPartitionID = (int) o;
        if (myPartitionID == partitionID) {
            Wrappers.IteratorWrapper<T> sparkIt = (Wrappers.IteratorWrapper) o2;
            List<T> list = new ArrayList<>();
            while (sparkIt.hasNext())
                list.add(sparkIt.next());
            Collections.shuffle(list, rand);
            return list.iterator();
        }
        return Collections.emptyIterator();
    }
}

class TakeSampleFunction<V> extends AbstractFunction1<scala.collection.Iterator<V>, List<V>> implements Serializable {

    private int start_id;
    private int end_id;

    TakeSampleFunction(int start_id, int end_id) {
        this.start_id = start_id;
        this.end_id = end_id;
    }

    @Override
    public List<V> apply(scala.collection.Iterator<V> iterator) {

        List<V> list = new ArrayList<>(end_id - start_id);
        int count = 0;
        V element;
        //sample from start_id to end_id
        while (iterator.hasNext()) {
            element = iterator.next();
            if (count >= start_id & count < end_id)
                list.add(element);
            count++;
            if (count >= end_id)
                break;
        }
        return list;
    }
}
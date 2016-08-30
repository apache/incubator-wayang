package org.qcri.rheem.spark.operators;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.execution.SparkExecutor;
import scala.collection.JavaConversions;
import scala.collection.convert.Wrappers;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.*;


/**
 * Spark implementation of the {@link SparkShufflePartitionSampleOperator}.
 */
public class SparkShufflePartitionSampleOperator<Type>
        extends SampleOperator<Type>
        implements SparkExecutionOperator {

    private final Random rand = new Random();

    private int partitionID = 0;

    private int tupleID = 0;

    /**
     * Creates a new instance.
     */
    public SparkShufflePartitionSampleOperator(int sampleSize, DataSetType<Type> type) {
        super(sampleSize, type, Methods.SHUFFLE_FIRST);
    }

    /**
     * Creates a new instance.
     */
    public SparkShufflePartitionSampleOperator(int sampleSize, long datasetSize, DataSetType<Type> type) {
        super(sampleSize, datasetSize, type, Methods.SHUFFLE_FIRST);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkShufflePartitionSampleOperator(SampleOperator<Type> that) {
        super(that);
        assert that.getSampleMethod() == Methods.SHUFFLE_FIRST || that.getSampleMethod() == Methods.ANY;
    }

    @Override
    public void evaluate(ChannelInstance[] inputs,
                         ChannelInstance[] outputs,
                         SparkExecutor sparkExecutor,
                         OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        RddChannel.Instance input = (RddChannel.Instance) inputs[0];

        JavaRDD<Type> inputRdd = input.provideRdd();
        if (datasetSize == 0) //total size of input dataset was not given
            datasetSize = inputRdd.cache().count();

        if (sampleSize >= datasetSize) { //return all and return
            ((CollectionChannel.Instance) outputs[0]).accept(inputRdd.collect());
            return;
        }

        List<Type> result;
        final SparkContext sparkContext = inputRdd.context();

        boolean miscalculated = false;
        do {
            if (tupleID == 0) {
                int nb_partitions = inputRdd.partitions().size();
                //choose a random partition
                partitionID = rand.nextInt(nb_partitions);
                inputRdd = inputRdd.<Type>mapPartitionsWithIndex(new ShufflePartition<>(partitionID), true).cache();
                miscalculated = false;
            }
            //read sequentially from partitionID
            List<Integer> pars = new ArrayList<>(1);
            pars.add(partitionID);
            Object samples = sparkContext.runJob(inputRdd.rdd(),
                    new TakeSampleFunction(tupleID, ((int) (tupleID + sampleSize))),
                    (scala.collection.Seq) JavaConversions.asScalaBuffer(pars),
                    true, scala.reflect.ClassTag$.MODULE$.apply(List.class));

            tupleID++;
            result = ((List<Type>[]) samples)[0];
            if (result == null) { //we reached end of partition, start again
                miscalculated = true;
                tupleID = 0;
            }
        } while (miscalculated);

        // assuming the sample is small better use a collection instance, the optimizer can transform the output if necessary
        ((CollectionChannel.Instance) outputs[0]).accept(result);

    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkShufflePartitionSampleOperator<>(this.sampleSize, this.getType());
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(org.qcri.rheem.core.api.Configuration configuration) {
        // NB: This was not measured but is guesswork, adapted from SparkFilterOperator.
        final NestableLoadProfileEstimator mainEstimator = new NestableLoadProfileEstimator(
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 700 * inputCards[0] + 500000000L),
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 10000),
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 0),
                new DefaultLoadEstimator(1, 1, .9d, (inputCards, outputCards) -> 0),
                (in, out) -> 0.23d,
                550
        );

        return Optional.of(mainEstimator);
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
    public boolean isExecutedEagerly() {
        return false;
    }
}

class ShufflePartition<V, T, R> implements Function2<V, T, R> {

    private int partitionID;

    ShufflePartition(int partitionID) {
        this.partitionID = partitionID;
    }

    @Override
    public Object call(Object o, Object o2) throws Exception {
        int myPartitionID = (int) o;
        if (myPartitionID == partitionID) {
            Wrappers.IteratorWrapper<T> sparkIt = (Wrappers.IteratorWrapper) o2;
            List<T> list = new ArrayList<>();
            while (sparkIt.hasNext())
                list.add(sparkIt.next());
            Collections.shuffle(list);
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

        //sampling
        List<V> list = new ArrayList<>(end_id - start_id);
        int count = 0;
        V element;

        while (iterator.hasNext()) {
            element = iterator.next();
            if (count >= start_id & count < end_id)
                list.add(element);
            count++;
            if (count > end_id)
                break;
        }
        if (count < end_id)
            return null; //miscalculated

        return list;
    }
}
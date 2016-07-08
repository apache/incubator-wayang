package org.qcri.rheem.spark.operators;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * Spark implementation of the {@link SparkRandomPartitionSampleOperator}. Sampling with replacement (i.e., the sample may contain duplicates)
 */
public class SparkRandomPartitionSampleOperator<Type>
        extends SampleOperator<Type>
        implements SparkExecutionOperator {

    protected Random rand = new Random();
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Creates a new instance.
     *
     * @param sampleSize
     */
    public SparkRandomPartitionSampleOperator(int sampleSize, DataSetType type) {
        super(sampleSize, type, Methods.RANDOM);
    }

    /**
     * Creates a new instance.
     *
     * @param sampleSize
     * @param datasetSize
     */
    public SparkRandomPartitionSampleOperator(int sampleSize, long datasetSize, DataSetType type) {
        super(sampleSize, datasetSize, type, Methods.RANDOM);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkRandomPartitionSampleOperator(SampleOperator<Type> that) {
        super(that);
    }

    int nb_partitions = 0;
    int partitionSize = 0;
    int parallel = 0;
    boolean first = true;
    int threshold = 5000;

    @Override
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        RddChannel.Instance input = (RddChannel.Instance) inputs[0];

        final JavaRDD<Object> inputRdd = input.provideRdd();
        if (datasetSize == 0) //total size of input dataset was not given
        {
            datasetSize = inputRdd.cache().count();
        }

        if (sampleSize >= datasetSize) { //return whole dataset
            ((CollectionChannel.Instance) outputs[0]).accept(inputRdd.collect());
            return;
        }

        List<Type> result;
        final SparkContext sparkContext = inputRdd.context();

        if (first) { //first time -> retrieve some statistics for partitions
            int tasks = sparkContext.defaultParallelism();
            nb_partitions = inputRdd.partitions().size();
            partitionSize = (int) Math.ceil((double) datasetSize / nb_partitions);
            parallel = Math.min(sampleSize, tasks);
            first = false;
        }

        if (sampleSize == 1) { //e.g., for SGD
            int pid = rand.nextInt(nb_partitions); //sample partition
            int tid = rand.nextInt(partitionSize); // sample item inside the partition
            List<Integer> partitions = Collections.singletonList(pid);
            Object samples = sparkContext.runJob(inputRdd.rdd(),
                    new PartitionSampleFunction(tid, ((tid + sampleSize))),
                    (scala.collection.Seq) JavaConversions.asScalaBuffer(partitions),
                    true, scala.reflect.ClassTag$.MODULE$.apply(List.class));
            result = ((List<Type>[]) samples)[0];
        } else {
            HashMap<Integer, ArrayList<Integer>> map = new HashMap<>(); //list should be ordered..
            for (int i = 0; i < sampleSize; i++) {
                int pid = rand.nextInt(nb_partitions); //sample partition
                int tid = rand.nextInt(partitionSize); // sample item inside the partition
                ArrayList<Integer> list;
                if ((list = map.get(pid)) == null) { //first time in partition pid
                    list = new ArrayList<>();
                    list.add(tid);
                    map.put(pid, list);
                } else {
                    list.add(tid);
                }
            }
            List<Type> allSamples = new ArrayList<>();

            ExecutorService executorService = Executors.newFixedThreadPool(map.size());

            Iterator<Integer> parts = map.keySet().iterator();
            List<Future<Object>> results = new ArrayList<>(map.size());

            while (parts.hasNext()) { //run for each partition
                int pid = parts.next();
                List<Integer> partitions = Collections.singletonList(pid);
                ArrayList list = map.get(pid);
                Collections.sort(list); // order list of tids

                // Start a thread
                results.add(executorService.submit(() ->
                        sparkContext.runJob(inputRdd.rdd(),
                                new PartitionSampleListFunction(list),
                                (scala.collection.Seq) JavaConversions.asScalaBuffer(partitions),
                                true, scala.reflect.ClassTag$.MODULE$.apply(List.class))));
            }

            for (int i = 0; i < map.size(); i++)
                try {
                    allSamples.addAll(((List<Type>[]) results.get(i).get())[0]);
                } catch (InterruptedException e) {
                    this.logger.error("Random partition sampling failed due to threads.", e);
                } catch (ExecutionException e) {
                    throw new RheemException("Random partition sampling failed.", e);
                }

            executorService.shutdown();
            // Wait until all threads are finish
            result = allSamples;
        }

        // assuming the sample is small better use a collection instance, the optimizer can transform the output if necessary
        ((CollectionChannel.Instance) outputs[0]).accept(result);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkRandomPartitionSampleOperator<>(this.sampleSize, this.getType());
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
}

class PartitionSampleFunction<V> extends AbstractFunction1<scala.collection.Iterator<V>, List<V>> implements Serializable {

    int start_id;
    int end_id;

    PartitionSampleFunction(int start_id, int end_id) {
        this.start_id = start_id;
        this.end_id = end_id;
    }

    @Override
    public List<V> apply(scala.collection.Iterator<V> iterator) {

        //sampling
        List<V> list = new ArrayList<>(end_id - start_id);
        int index = 0;
        V element = null;
        while (iterator.hasNext()) {
            element = iterator.next();
            if (index >= start_id & index < end_id)
                list.add(element);
            index++;
            if (index > end_id)
                break;
        }
        if (index < end_id)
            list.add(element); //take last element
        ///FIXME: there are cases were the list will be smaller because of miscalculation of the partition size for mini-batch

        return list;
    }
}

class PartitionSampleListFunction<V> extends AbstractFunction1<scala.collection.Iterator<V>, List<V>> implements Serializable {

    ArrayList<Integer> ids; //ids should be sorted

    PartitionSampleListFunction(ArrayList<Integer> ids) {
        this.ids = ids;
    }

    @Override
    public List<V> apply(scala.collection.Iterator<V> iterator) {
        //sampling
        List<V> list = new ArrayList<>(ids.size());
        int count = 0;
        V element = null;
        int index = 0;
        while (iterator.hasNext()) {
            element = iterator.next();
            if (count == ids.get(index)) { // we have to take exactly ids.size() elements
                list.add(element);
                if (index == ids.size() - 1) //if we took all elements we need, break
                    break;
                index++;
                if (ids.get(index).equals(ids.get(index - 1))) { // for duplicates
                    list.add(element);
                    if (index == ids.size() - 1) //if we took all elements we need, break
                        break;
                    index++;
                }

            }
            count++;
        }
        if (list.size() < ids.size()) //take last element, if we miscalculated
            list.add(element);
        return list;
    }
}

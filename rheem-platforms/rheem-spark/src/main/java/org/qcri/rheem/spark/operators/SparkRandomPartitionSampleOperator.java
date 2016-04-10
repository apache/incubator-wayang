package org.qcri.rheem.spark.operators;

import gnu.trove.map.hash.THashMap;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * Spark implementation of the {@link SparkRandomPartitionSampleOperator}.
 */
public class SparkRandomPartitionSampleOperator<Type>
        extends SampleOperator<Type>
        implements SparkExecutionOperator {

    long totalSize;

    /**
     * Creates a new instance.
     *
     * @param predicateDescriptor
     */
    public SparkRandomPartitionSampleOperator(long sampleSize, DataSetType type,
                                              PredicateDescriptor<Type> predicateDescriptor) {
        super(sampleSize, predicateDescriptor, type);
    }

    /**
     * Creates a new instance.
     *
     * @param sampleSize
     */
    public SparkRandomPartitionSampleOperator(long sampleSize, long totalSize, DataSetType type) {
        super(sampleSize, null, type);
        this.totalSize = totalSize;
    }

    int nb_partitions = 0;
    int partitionSize = 0;
    int parallel = 0;
    boolean first = true;

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        if (sampleSize >= totalSize) { //return all and return
            outputs[0].acceptRdd(inputs[0].provideRdd());
            return;
        }

        List<Type> result;
        final JavaRDD<Type> inputRdd = inputs[0].provideRdd();
        final SparkContext sparkContext = inputRdd.context();

        if (first) { //first time -> retrieve some statistics for partitions
            int tasks = sparkContext.defaultParallelism();
            nb_partitions = inputRdd.partitions().size();
            partitionSize = (int) Math.ceil((double) totalSize / nb_partitions);
            parallel = Math.min((int) sampleSize, tasks);
            first = false;
        }

        if (sampleSize == 1) { //e.g., for SGD
            int pid = rand.nextInt(nb_partitions); //sample partition
            int tid = rand.nextInt(partitionSize); // sample item inside the partition
            List<Integer> partitions = Collections.singletonList(pid);
            Object samples = sparkContext.runJob(inputRdd.rdd(),
                    new PartitionSampleFunction(tid, ((int) (tid + sampleSize))),
                    (scala.collection.Seq) JavaConversions.asScalaBuffer(partitions),
                    true, scala.reflect.ClassTag$.MODULE$.apply(List.class));
            result = ((List<Type>[]) samples)[0];
        }
        else {
            THashMap<Integer, ArrayList<Integer>> map = new THashMap<>(); //list should be ordered..
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
                results.add(executorService.submit( () ->
                        sparkContext.runJob(inputRdd.rdd(),
                                new PartitionSampleListFunction(list),
                                (scala.collection.Seq) JavaConversions.asScalaBuffer(partitions),
                                true, scala.reflect.ClassTag$.MODULE$.apply(List.class))));
            }

            for (int i = 0; i < map.size(); i ++)
                try {
                    allSamples.addAll(((List<Type>[]) results.get(i).get())[0]);
                } catch (InterruptedException e) { //TODO: check with sebastian if we need some other kind of exception
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

            executorService.shutdown();
            // Wait until all threads are finish
            result = allSamples;
        }

        final JavaRDD<Type> outputRdd = sparkExecutor.sc.parallelize(result); //FIXME: this is not efficient
        outputs[0].acceptRdd(outputRdd);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkRandomPartitionSampleOperator<>(this.sampleSize, this.getInputType(), this.getPredicateDescriptor());
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
        int count = 0;
        V element = null;
        while (iterator.hasNext()) {
            element = iterator.next();
            if (count >= start_id & count < end_id)
                list.add(element);
            count++;
            if (count > end_id)
                break;
        }
        if (count < end_id)
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

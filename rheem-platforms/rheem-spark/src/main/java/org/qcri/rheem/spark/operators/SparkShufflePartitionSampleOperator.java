package org.qcri.rheem.spark.operators;

import gnu.trove.map.hash.THashMap;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import org.qcri.rheem.spark.platform.SparkExecutor;
import scala.collection.JavaConversions;
import scala.collection.convert.Wrappers;
import scala.runtime.AbstractFunction1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * Spark implementation of the {@link SparkShufflePartitionSampleOperator}.
 */
public class SparkShufflePartitionSampleOperator<Type>
        extends SampleOperator<Type>
        implements SparkExecutionOperator {

    long totalSize;
    int partitionID = 0;
    int tupleID = 0;

    /**
     * Creates a new instance.
     *
     * @param predicateDescriptor
     */
    public SparkShufflePartitionSampleOperator(long sampleSize, DataSetType type,
                                               PredicateDescriptor<Type> predicateDescriptor) {
        super(sampleSize, predicateDescriptor, type);
    }

    /**
     * Creates a new instance.
     *
     * @param sampleSize
     */
    public SparkShufflePartitionSampleOperator(long sampleSize, long totalSize, DataSetType type) {
        super(sampleSize, null, type);
        this.totalSize = totalSize;
    }

    int nb_partitions = 0;

    @Override
    public void evaluate(ChannelExecutor[] inputs, ChannelExecutor[] outputs, FunctionCompiler compiler, SparkExecutor sparkExecutor) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        if (sampleSize >= totalSize) { //return all and return
            outputs[0].acceptRdd(inputs[0].provideRdd());
            return;
        }

        List<Type> result;
        JavaRDD<Type> inputRdd = inputs[0].provideRdd();
        final SparkContext sparkContext = inputRdd.context();

        boolean miscalculated = false;
        do {
            if (tupleID == 0) {
                nb_partitions = inputRdd.partitions().size();
                //choose a random partition
//                partitionID = rand.nextInt(nb_partitions);
                partitionID = 0;
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

        final JavaRDD<Type> outputRdd = sparkExecutor.sc.parallelize(result); //FIXME: this is not efficient
        outputs[0].acceptRdd(outputRdd);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkShufflePartitionSampleOperator<>(this.sampleSize, this.getInputType(), this.getPredicateDescriptor());
    }
}

class ShufflePartition<V, T, R> implements Function2<V, T, R> {

    int partitionID;

    public ShufflePartition(int partitionID) {
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

    int start_id;
    int end_id;

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
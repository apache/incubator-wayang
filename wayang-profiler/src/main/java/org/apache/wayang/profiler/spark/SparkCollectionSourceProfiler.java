package org.apache.incubator.wayang.profiler.spark;

import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.types.DataSetType;
import org.apache.incubator.wayang.spark.operators.SparkCollectionSource;
import org.apache.incubator.wayang.spark.operators.SparkTextFileSource;

import java.util.ArrayList;
import java.util.function.Supplier;

/**
 * {@link SparkOperatorProfiler} for the {@link SparkTextFileSource}.
 */
public class SparkCollectionSourceProfiler extends SparkSourceProfiler {

    private final ArrayList<Object> collection;

    public <T extends Object> SparkCollectionSourceProfiler(Configuration configuration,
                                             Supplier<T> dataQuantumGenerator,
                                             DataSetType<T> outputType) {
        this(new ArrayList<>(), configuration, dataQuantumGenerator, outputType);
    }

    private <T extends Object> SparkCollectionSourceProfiler(ArrayList<T> collection,
                                                             Configuration configuration,
                                                             Supplier<T> dataQuantumGenerator,
                                                             DataSetType<T> outputType) {
        super(() -> new SparkCollectionSource<>(collection, outputType), configuration, dataQuantumGenerator);
        this.collection = (ArrayList<Object>) collection;
    }

    @Override
    protected void prepareInput(int inputIndex, long inputCardinality) {
        assert inputIndex == 0;
        assert inputCardinality <= Integer.MAX_VALUE;

        this.collection.clear();
        this.collection.ensureCapacity((int) inputCardinality);
        final Supplier<?> supplier = this.dataQuantumGenerators.get(0);
        for (long i = 0; i < inputCardinality; i++) {
            this.collection.add(supplier.get());
        }
    }

    @Override
    public void cleanUp() {
        super.cleanUp();

        this.collection.clear();
        this.collection.trimToSize();
    }
}

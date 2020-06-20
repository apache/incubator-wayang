package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.function.ConsumerDescriptor;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * This sink executes a callback on each received data unit into a Java {@link Collection}.
 */
public class LocalCallbackSink<T> extends UnarySink<T> {

    protected final Consumer<T> callback;

    protected final ConsumerDescriptor.SerializableConsumer<T> callbackDescriptor;

    protected Collection<T> collector;

    public static <T> LocalCallbackSink<T> createCollectingSink(Collection<T> collector, DataSetType<T> type) {
        return new LocalCallbackSink<>((ConsumerDescriptor.SerializableConsumer<T>)collector::add, type).setCollector(collector);
    }

    public static <T> LocalCallbackSink<T> createCollectingSink(Collection<T> collector, Class<T> typeClass) {
        return new LocalCallbackSink<>((ConsumerDescriptor.SerializableConsumer<T>)collector::add, typeClass).setCollector(collector);
    }

    public static <T> LocalCallbackSink<T> createStdoutSink(DataSetType<T> type) {
        return new LocalCallbackSink<>((ConsumerDescriptor.SerializableConsumer<T>)System.out::println, type);
    }

    public static <T> LocalCallbackSink<T> createStdoutSink(Class<T> typeClass) {
        return new LocalCallbackSink<>((ConsumerDescriptor.SerializableConsumer<T>)System.out::println, typeClass);
    }

    /**
     * Creates a new instance.
     *
     * @param callback callback that is executed locally for each incoming data unit
     * @param type     type of the incoming elements
     */
    public LocalCallbackSink(Consumer<T> callback, DataSetType<T> type) {
        super(type);
        this.callback = callback;
        this.callbackDescriptor = null;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public LocalCallbackSink(LocalCallbackSink<T> that) {
        super(that);
        this.callback = that.getCallback();
        this.callbackDescriptor = that.getCallbackDescriptor();
        this.collector = that.collector;
    }


    /**
     * Creates a new instance.
     *
     * @param callback callback that is executed locally for each incoming data unit
     * @param typeClass     type of the incoming elements
     */
    public LocalCallbackSink(Consumer<T> callback, Class<T> typeClass) {
        this(callback, DataSetType.createDefault(typeClass));
    }

    /**
     * Creates a new instance.
     */
    public LocalCallbackSink(ConsumerDescriptor.SerializableConsumer<T> consumerDescriptor, Class<T> typeClass) {
        super(DataSetType.createDefault(BasicDataUnitType.createBasic(typeClass)), true);
        this.callbackDescriptor = consumerDescriptor;
        this.callback = consumerDescriptor;
    }

    /**
     * Creates a new instance.
     *
     * @param type type of the dataunit elements
     */
    public LocalCallbackSink(ConsumerDescriptor.SerializableConsumer<T> consumerDescriptor, DataSetType<T> type) {
        super(type, true);
        this.callbackDescriptor = consumerDescriptor;
        this.callback = consumerDescriptor;
    }

    public LocalCallbackSink<T> setCollector(Collection<T> collector){
        this.collector = collector;
        return this;
    }

    /**
     *  Convnience constructor, defaults to StdoutSink
     */
    public LocalCallbackSink(Class<T> typeClass ){
        this((FunctionDescriptor.SerializableConsumer<T>)System.out::println, typeClass);
    }

    public Consumer<T> getCallback() {
        return this.callback;
    }

    public ConsumerDescriptor.SerializableConsumer<T> getCallbackDescriptor(){
        return this.callbackDescriptor;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return super.createCardinalityEstimator(outputIndex, configuration);
    }
}

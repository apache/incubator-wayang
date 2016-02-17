package org.qcri.rheem.java.channels;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link ChannelExecutor} implementation for verification purposes.
 */
public class TestChannelExecutor implements ChannelExecutor {

    private Collection collection;

    public TestChannelExecutor() {
    }

    public TestChannelExecutor(Stream inputStream) {
        this();
        this.acceptStream(inputStream);
    }

    public TestChannelExecutor(Collection<?> inputCollection) {
        this();
        this.collection = inputCollection;
    }

    @Override
    public void acceptStream(Stream<?> stream) {
        this.collection = stream.collect(Collectors.toList());
    }

    @Override
    public void acceptCollection(Collection<?> collection) {
        this.collection = collection;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Stream<?> provideStream() {
        return this.collection.stream();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<?> provideCollection() {
        return this.collection;
    }

    @Override
    public boolean canProvideCollection() {
        return true;
    }
}

package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.Source;
import org.qcri.rheem.core.types.DataSet;

import java.util.Collection;

/**
 * This source takes as input a Java {@link java.util.Collection}.
 */
public class CollectionSource implements Source {

    private final OutputSlot outputSlot;

    private final OutputSlot[] outputSlots;

    protected final Collection<?> collection;

    public CollectionSource(Collection<?> collection, DataSet type) {
        this.outputSlot = new OutputSlot<>("elements", this, type);
        this.outputSlots = new OutputSlot[] { this.outputSlot };
        this.collection = collection;
    }

    @Override
    public OutputSlot[] getAllOutputs() {
        return outputSlots;
    }

    public DataSet getType() {
        return this.outputSlot.getType();
    }

    public Collection<?> getCollection() {
        return collection;
    }
}

package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.plan.ActualOperator;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.UnarySource;
import org.qcri.rheem.core.types.DataSet;

import java.util.Collection;

/**
 * This source takes as input a Java {@link java.util.Collection}.
 */
public class CollectionSource<T> extends UnarySource<T> implements ActualOperator {

    protected final Collection<T> collection;

    public CollectionSource(Collection<T> collection, DataSet type) {
        super(type, null);
        this.collection = collection;
    }

    public Collection<T> getCollection() {
        return collection;
    }
}

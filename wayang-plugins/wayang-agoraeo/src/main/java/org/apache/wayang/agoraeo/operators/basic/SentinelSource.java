package org.apache.wayang.agoraeo.operators.basic;

import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;

import java.util.Iterator;

public class SentinelSource<Type> extends UnarySource<Type> {

    private Iterator<Type> iterator;

    /**
     * Default construct
     *
     * @param iterator {@link Iterator} Instance produced by this source
     * @param _class type of the data produced by the iterator
     */
    public SentinelSource(Iterator<Type> iterator, Class<Type> _class){
        super(DataSetType.createDefault(_class));
        this.iterator = iterator;
    }

    public SentinelSource(SentinelSource<Type> that) {
        super(that);
        this.iterator = that.getIterator();
    }

    public Iterator<Type> getIterator() {
        return this.iterator;
    }

}

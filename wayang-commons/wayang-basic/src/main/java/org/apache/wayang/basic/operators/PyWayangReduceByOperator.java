package org.apache.wayang.basic.operators;

import com.google.protobuf.ByteString;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Map;

public class PyWayangReduceByOperator<Type, Key> extends UnaryToUnaryOperator<Type, Type> {

    protected final Map<String, String> parameters;
    protected final ByteString reduceDescriptor;

    public PyWayangReduceByOperator(Map<String, String> parameters,
                                       ByteString reduceDescriptor,
                                       DataSetType<Type> inputType, DataSetType<Type> outputType, boolean isSupportingBroadcastInputs) {
        super(inputType, outputType, isSupportingBroadcastInputs);
        this.parameters = parameters;
        this.reduceDescriptor = reduceDescriptor;
    }

    public PyWayangReduceByOperator(Map<String, String> parameters,
                                    ByteString reduceDescriptor,
                                    Class<Type> inputType, Class<Type> outputType, boolean isSupportingBroadcastInputs) {
        super(DataSetType.createDefault(inputType), DataSetType.createDefault(outputType), isSupportingBroadcastInputs);
        this.parameters = parameters;
        this.reduceDescriptor = reduceDescriptor;
    }

    public PyWayangReduceByOperator(PyWayangReduceByOperator<Type, Type> that) {
        super(that);
        this.parameters = that.getParameters();
        this.reduceDescriptor = that.getReduceDescriptor();
    }

    public Map<String, String>  getParameters() {
        return this.parameters;
    }

    public ByteString getReduceDescriptor() {
        return this.reduceDescriptor;
    }

}
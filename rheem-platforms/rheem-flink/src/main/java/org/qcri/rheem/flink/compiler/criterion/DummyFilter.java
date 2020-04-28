package org.qcri.rheem.flink.compiler.criterion;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

/**
 * Create a {@Link FilterFunction} that remove the elements null
 */
public class DummyFilter<InputType> implements FilterFunction<InputType>, ResultTypeQueryable<InputType> {


    public final Class<InputType>  inputTypeClass;
    private final TypeInformation<InputType> typeInformationInput;

    public DummyFilter(Class<InputType>  inputTypeClass){
        this.inputTypeClass  = inputTypeClass;
        this.typeInformationInput   = TypeInformation.of(this.inputTypeClass);
    }


    @Override
    public boolean filter(InputType inputType) throws Exception {
        return inputType != null;
    }

    @Override
    public TypeInformation<InputType> getProducedType() {
        return this.typeInformationInput;
    }

}

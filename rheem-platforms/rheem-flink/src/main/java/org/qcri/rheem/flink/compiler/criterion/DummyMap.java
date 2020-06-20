package org.qcri.rheem.flink.compiler.criterion;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

/**
 * Class create a {@Link MapFunction} that genereta only null as convertion
 */
public class DummyMap<InputType, OutputType> implements MapFunction<InputType, OutputType>, ResultTypeQueryable<OutputType> {

    public final Class<InputType>  inputTypeClass;
    public final Class<OutputType> outputTypeClass;
    private final TypeInformation<InputType>  typeInformationInput;
    private final TypeInformation<OutputType> typeInformationOutput;

    public DummyMap(Class<InputType>  inputTypeClass, Class<OutputType> outputTypeClass){
        this.inputTypeClass  = inputTypeClass;
        this.outputTypeClass = outputTypeClass;
        this.typeInformationInput   = TypeInformation.of(this.inputTypeClass);
        this.typeInformationOutput  = TypeInformation.of(this.outputTypeClass);
    }


    @Override
    public OutputType map(InputType inputType) throws Exception {
        return null;
    }

    @Override
    public TypeInformation<OutputType> getProducedType() {
        return this.typeInformationOutput;
    }


}

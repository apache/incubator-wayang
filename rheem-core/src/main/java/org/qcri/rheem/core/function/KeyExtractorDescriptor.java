package org.qcri.rheem.core.function;

import org.qcri.rheem.core.types.BasicDataUnitType;

import java.util.function.Function;

/**
 * This descriptor  represent functions that extract key from a data units.
 *
 * @param <Input>  input type of the transformation function
 * @param <KeyType> output type of the transformation function
 */

public class KeyExtractorDescriptor <Input, KeyType> extends TransformationDescriptor<Input, KeyType> {

    public KeyExtractorDescriptor(Function<Input, KeyType> javaImplementation,
                                  BasicDataUnitType inputType,
                                  BasicDataUnitType keyType) {
        super(javaImplementation, inputType, keyType);
    }

    @Override
    public KeyExtractorDescriptor<Object, Object> unchecked() {
        return (KeyExtractorDescriptor<Object, Object>) this;
    }
}
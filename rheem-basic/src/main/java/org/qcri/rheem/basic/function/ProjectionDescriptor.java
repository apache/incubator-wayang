package org.qcri.rheem.basic.function;

import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.types.BasicDataUnitType;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * This descriptor pertains to projections. It takes field names of the input type to describe the projection.
 */
public class ProjectionDescriptor<Input, Output> extends TransformationDescriptor<Input, Output> {

    private final List<String> fieldNames;

    public ProjectionDescriptor(BasicDataUnitType inputType, BasicDataUnitType outputType, String... fieldNames) {
        super(createJavaImplementation(fieldNames, inputType), inputType, outputType);
        this.fieldNames = Collections.unmodifiableList(Arrays.asList(fieldNames));
    }

    private static <Input, Output> Function<Input, Output> createJavaImplementation(String[] fieldNames,
                                                                                    BasicDataUnitType inputType) {
        // Get the name of the field to be projected.
        if (fieldNames.length != 1) {
            throw new IllegalStateException("The projection descriptor currently supports only a single field.");
        }
        String fieldName = fieldNames[0];

        // Get the input class.
        final Class<?> typeClass = inputType.getTypeClass();

        // Find the projection field via reflection.
        final Field field;
        try {
            field = typeClass.getField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("The configuration of the projection seems to be illegal.", e);
        }

        return in -> {
            try {
                return (Output) field.get(in);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Projection failed.", e);
            }
        };
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }
}

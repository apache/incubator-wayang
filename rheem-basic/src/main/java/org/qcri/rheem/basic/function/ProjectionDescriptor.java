package org.qcri.rheem.basic.function;

import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.types.BasicDataUnitType;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * This descriptor pertains to projections. It takes field names of the input type to describe the projection.
 */
public class ProjectionDescriptor<Input, Output> extends TransformationDescriptor<Input, Output> {

    private final List<String> fieldNames;

    public ProjectionDescriptor(BasicDataUnitType inputType, BasicDataUnitType outputType, String... fieldNames) {
        super(createJavaImplementation(fieldNames, inputType), inputType, outputType);
        this.fieldNames = Collections.unmodifiableList(Arrays.asList(fieldNames));
    }

    private static <Input, Output> FunctionDescriptor.SerializableFunction<Input, Output>
    createJavaImplementation(String[] fieldNames, BasicDataUnitType inputType) {
        // Get the name of the field to be projected.
        if (fieldNames.length != 1) {
            throw new IllegalStateException("The projection descriptor currently supports only a single field.");
        }
        String fieldName = fieldNames[0];
        return new JavaFunction<>(fieldName);
    }

    public List<String> getFieldNames() {
        return this.fieldNames;
    }

    private static class JavaFunction<Input, Output> implements FunctionDescriptor.SerializableFunction<Input, Output> {

        private final String fieldName;

        private Field field;

        private JavaFunction(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        public Output apply(Input input) {
            // Initialization code.
            if (this.field == null) {
                // Get the input class.
                final Class<?> typeClass = input.getClass();

                // Find the projection field via reflection.
                try {
                    this.field = typeClass.getField(this.fieldName);
                } catch (NoSuchFieldException e) {
                    throw new IllegalStateException("The configuration of the projection seems to be illegal.", e);
                }
            }

            // Actual function.
            try {
                return (Output) this.field.get(input);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Illegal projection function.", e);
            }
        }
    }
}

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

    private List<String> fieldNames;

    private final List<Integer> fieldIndexes;

    public Boolean isProjectByIndexes() {
        return projectByIndexes;
    }

    private Boolean projectByIndexes = false;

    public ProjectionDescriptor(Class<Input> inputTypeClass,
                                Class<Output> outputTypeClass,
                                String... fieldNames) {
        this(BasicDataUnitType.createBasic(inputTypeClass),
                BasicDataUnitType.createBasic(outputTypeClass),
                fieldNames);
    }

    public ProjectionDescriptor(Class<Input> inputTypeClass,
                                Class<Output> outputTypeClass,
                                Integer... fieldIndexes) {
        this(BasicDataUnitType.createBasic(inputTypeClass),
                BasicDataUnitType.createBasic(outputTypeClass),
                fieldIndexes);
    }

    public ProjectionDescriptor(BasicDataUnitType inputType, BasicDataUnitType outputType, String... fieldNames) {
        super(createJavaImplementation(fieldNames, inputType), inputType, outputType);
        this.fieldNames = Collections.unmodifiableList(Arrays.asList(fieldNames));
        this.fieldIndexes = null;
    }

    public ProjectionDescriptor(BasicDataUnitType inputType, BasicDataUnitType outputType, Integer... fieldIndexes) {
        super(createJavaImplementation(fieldIndexes, inputType), inputType, outputType);
        this.fieldIndexes = Collections.unmodifiableList(Arrays.asList(fieldIndexes));
        this.fieldNames = null;
        projectByIndexes = true;
    }

    private static <Input, Output> FunctionDescriptor.SerializableFunction<Input, Output>
    createJavaImplementation(String[] fieldNames, BasicDataUnitType inputType) {
        // Get the names of the fields to be projected.
        if (fieldNames.length != 1) {
            //throw new IllegalStateException("The projection descriptor currently supports only a single field.");
        }
        String fieldName = fieldNames[0];
        return new JavaFunction<>(fieldName);
    }

    private static <Input, Output> FunctionDescriptor.SerializableFunction<Input, Output>
    createJavaImplementation(Integer[] fieldIndexes, BasicDataUnitType inputType) {
        // Get the indexes of the fields to be projected.
        if (fieldIndexes.length != 1) {
            //throw new IllegalStateException("The projection descriptor currently supports only a single field.");
        }
        Integer fieldIndex = fieldIndexes[0];
        return new JavaFunction<>(fieldIndex);
    }

    public List<String> getFieldNames() {
        return this.fieldNames;
    }
    public void setFieldNames(List<String> fieldNames) {
         this.fieldNames = fieldNames;
    }
    public List<Integer> getFieldIndexes() {
        return fieldIndexes;
    }

    // TODO: Revise implementation to support multiple field projection, by names and indexes.
    private static class JavaFunction<Input, Output> implements FunctionDescriptor.SerializableFunction<Input, Output> {

        private final String fieldName;

        private final Integer fieldIndex;

        private Field field;

        private JavaFunction(String fieldName) {
            this.fieldName = fieldName;
            this.fieldIndex = null;
        }

        private JavaFunction(Integer fieldIndex) {
            this.fieldName = null;
            this.fieldIndex = fieldIndex;
        }

        @Override
        public Output apply(Input input) {
            // Initialization code.
            if (this.field == null) {
                // Get the input class.
                final Class<?> typeClass = input.getClass();

                // Find the projection field via reflection.
                try {
                    if (this.fieldName!=null)
                        this.field = typeClass.getField(this.fieldName);
                    else
                        this.field = typeClass.getFields()[fieldIndex];
                } catch (Exception e) {
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

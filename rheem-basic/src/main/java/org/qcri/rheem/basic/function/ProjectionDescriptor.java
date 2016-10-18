package org.qcri.rheem.basic.function;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.types.RecordType;
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

    /**
     * Creates a new instance.
     *
     * @param inputTypeClass  input type
     * @param outputTypeClass output type
     * @param fieldNames      names of the fields to be projected
     */
    public ProjectionDescriptor(Class<Input> inputTypeClass,
                                Class<Output> outputTypeClass,
                                String... fieldNames) {
        this(BasicDataUnitType.createBasic(inputTypeClass),
                BasicDataUnitType.createBasic(outputTypeClass),
                fieldNames);
    }

    /**
     * Creates a new instance.
     *
     * @param inputType  input type
     * @param outputType output type
     * @param fieldNames names of the fields to be projected
     */
    public ProjectionDescriptor(BasicDataUnitType<Input> inputType, BasicDataUnitType<Output> outputType, String... fieldNames) {
        this(createPojoJavaImplementation(fieldNames, inputType),
                Collections.unmodifiableList(Arrays.asList(fieldNames)),
                inputType,
                outputType);
    }

    /**
     * Basic constructor.
     *
     * @param javaImplementation Java-based implementation of the projection
     * @param fieldNames         names of the fields to be projected
     * @param inputType          input {@link BasicDataUnitType}
     * @param outputType         output {@link BasicDataUnitType}
     */
    private ProjectionDescriptor(SerializableFunction<Input, Output> javaImplementation,
                                 List<String> fieldNames,
                                 BasicDataUnitType<Input> inputType,
                                 BasicDataUnitType<Output> outputType) {
        super(javaImplementation, inputType, outputType);
        this.fieldNames = fieldNames;
    }

    /**
     * Creates a new instance that specifically projects {@link Record}s.
     *
     * @param inputType  input {@link RecordType}
     * @param fieldNames names of fields to be projected
     * @return the new instance
     */
    public static ProjectionDescriptor<Record, Record> createForRecords(RecordType inputType, String... fieldNames) {
        final SerializableFunction<Record, Record> javaImplementation = createRecordJavaImplementation(fieldNames, inputType);
        return new ProjectionDescriptor<>(
                javaImplementation,
                Arrays.asList(fieldNames),
                inputType,
                new RecordType(fieldNames)
        );
    }

    private static <Input, Output> FunctionDescriptor.SerializableFunction<Input, Output>
    createPojoJavaImplementation(String[] fieldNames, BasicDataUnitType<Input> inputType) {
        // Get the names of the fields to be projected.
        if (fieldNames.length != 1) {
            return t -> {
                throw new IllegalStateException("The projection descriptor currently supports only a single field.");
            };
        }
        String fieldName = fieldNames[0];
        return new PojoImplementation<>(fieldName);
    }

    private static FunctionDescriptor.SerializableFunction<Record, Record>
    createRecordJavaImplementation(String[] fieldNames, RecordType inputType) {
        return new RecordImplementation(inputType, fieldNames);
    }

    /**
     * Transforms an array of {@link RecordType} field names to indices.
     *
     * @param recordType that maps field names to indices
     * @param fieldNames the field names
     * @return the field indices
     */
    private static int[] toIndices(RecordType recordType, String[] fieldNames) {
        int[] fieldIndices = new int[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = fieldNames[i];
            fieldIndices[i] = recordType.getIndex(fieldName);
        }
        return fieldIndices;
    }

    public List<String> getFieldNames() {
        return this.fieldNames;
    }

    /**
     * Java implementation of a projection on POJOs via reflection.
     */
    // TODO: Revise implementation to support multiple field projection, by names and indexes.
    private static class PojoImplementation<Input, Output> implements FunctionDescriptor.SerializableFunction<Input, Output> {

        private final String fieldName;

        private Field field;

        private PojoImplementation(String fieldName) {
            this.fieldName = fieldName;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Output apply(Input input) {
            // Initialization code.
            if (this.field == null) {

                // Get the input class.
                final Class<?> typeClass = input.getClass();

                // Find the projection field via reflection.
                try {
                    this.field = typeClass.getField(this.fieldName);
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

    /**
     * Java implementation of a projection on {@link Record}s.
     */
    private static class RecordImplementation implements FunctionDescriptor.SerializableFunction<Record, Record> {

        /**
         * Indices of the fields to be projected.
         */
        private final int[] fieldIndices;

        /**
         * Creates a new instance.
         *
         * @param recordType {@link RecordType} of input {@link Record}s
         * @param fieldNames that should be projected on
         */
        private RecordImplementation(RecordType recordType, String... fieldNames) {
            this.fieldIndices = toIndices(recordType, fieldNames);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Record apply(Record input) {
            Object[] projectedFields = new Object[this.fieldIndices.length];
            for (int i = 0; i < this.fieldIndices.length; i++) {
                int fieldIndex = this.fieldIndices[i];
                projectedFields[i] = input.getField(fieldIndex);
            }
            return new Record(projectedFields);
        }
    }
}

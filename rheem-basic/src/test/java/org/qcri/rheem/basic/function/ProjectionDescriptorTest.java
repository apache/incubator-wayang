package org.qcri.rheem.basic.function;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.types.RecordType;

import java.util.function.Function;

/**
 * Tests for the {@link ProjectionDescriptor}.
 */
public class ProjectionDescriptorTest {

    @Test
    public void testPojoImplementation() {
        final ProjectionDescriptor<Pojo, String> stringDescriptor = new ProjectionDescriptor<>(Pojo.class, String.class, "string");
        final Function<Pojo, String> stringImplementation = stringDescriptor.getJavaImplementation();

        final ProjectionDescriptor<Pojo, Integer> integerDescriptor = new ProjectionDescriptor<>(Pojo.class, Integer.class, "integer");
        final Function<Pojo, Integer> integerImplementation = integerDescriptor.getJavaImplementation();

        Assert.assertEquals(
                "testValue",
                stringImplementation.apply(new Pojo("testValue", 1))
        );
        Assert.assertEquals(
                null,
                stringImplementation.apply(new Pojo(null, 1))
        );
        Assert.assertEquals(
                Integer.valueOf(1),
                integerImplementation.apply(new Pojo("testValue", 1))
        );

    }

    @Test
    public void testRecordImplementation() {
        RecordType inputType = new RecordType("a", "b", "c");
        final ProjectionDescriptor<Record, Record> descriptor = ProjectionDescriptor.createForRecords(inputType, "c", "a");
        Assert.assertEquals(new RecordType("c", "a"), descriptor.getOutputType());

        final Function<Record, Record> javaImplementation = descriptor.getJavaImplementation();
        Assert.assertEquals(
                new Record("world", 10),
                javaImplementation.apply(new Record(10, "hello", "world"))
        );
    }

    public static class Pojo {

        public String string;

        public int integer;

        public Pojo(String string, int integer) {
            this.string = string;
            this.integer = integer;
        }
    }
}

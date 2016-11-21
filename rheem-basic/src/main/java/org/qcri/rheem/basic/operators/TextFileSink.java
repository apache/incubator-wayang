package org.qcri.rheem.basic.operators;

import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.costs.DefaultLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Objects;

/**
 * This {@link UnarySink} writes all incoming data quanta to a text file.
 */
public class TextFileSink<T> extends UnarySink<T> {

    protected final String textFileUrl;

    protected final TransformationDescriptor<T, String> formattingDescriptor;

    /**
     * Creates a new instance with default formatting.
     *
     * @param textFileUrl URL to file that should be written
     * @param typeClass   {@link Class} of incoming data quanta
     */
    public TextFileSink(String textFileUrl, Class<T> typeClass) {
        this(
                textFileUrl,
                new TransformationDescriptor<>(
                        Objects::toString,
                        typeClass,
                        String.class,
                        new NestableLoadProfileEstimator(
                                new DefaultLoadEstimator(1, 1, .99d, (in, out) -> 10 * in[0]),
                                new DefaultLoadEstimator(1, 1, .99d, (in, out) -> 1000)
                        )
                )
        );
    }


    /**
     * Creates a new instance.
     *
     * @param textFileUrl        URL to file that should be written
     * @param formattingFunction formats incoming data quanta to a {@link String} representation
     * @param typeClass          {@link Class} of incoming data quanta
     */
    public TextFileSink(String textFileUrl,
                        TransformationDescriptor.SerializableFunction<T, String> formattingFunction,
                        Class<T> typeClass) {
        this(
                textFileUrl,
                new TransformationDescriptor<>(formattingFunction, typeClass, String.class)
        );
    }

    /**
     * Creates a new instance.
     *
     * @param textFileUrl          URL to file that should be written
     * @param formattingDescriptor formats incoming data quanta to a {@link String} representation
     */
    public TextFileSink(String textFileUrl, TransformationDescriptor<T, String> formattingDescriptor) {
        super(DataSetType.createDefault(formattingDescriptor.getInputType()));
        this.textFileUrl = textFileUrl;
        this.formattingDescriptor = formattingDescriptor;
    }

    /**
     * Creates a copied instance.
     *
     * @param that should be copied
     */
    public TextFileSink(TextFileSink<T> that) {
        super(that);
        this.textFileUrl = that.textFileUrl;
        this.formattingDescriptor = that.formattingDescriptor;
    }

}

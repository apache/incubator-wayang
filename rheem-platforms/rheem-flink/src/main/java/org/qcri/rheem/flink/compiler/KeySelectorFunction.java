package org.qcri.rheem.flink.compiler;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.qcri.rheem.core.function.TransformationDescriptor;

import java.util.function.Function;

/**
 * Created by bertty on 17-07-17.
 */
public class KeySelectorFunction<T, K> implements KeySelector<T, K>, ResultTypeQueryable<K> {

    private final Function<T, K> impl;

    private final Class<K> key;

    private final TypeInformation<K> typeInformation;

    public KeySelectorFunction(TransformationDescriptor<T, K> transformationDescriptor) {

        this.impl = transformationDescriptor.getJavaImplementation();
        this.key  = transformationDescriptor.getOutputType().getTypeClass();
        this.typeInformation = TypeInformation.of(this.key);
    }

    public K getKey(T object){
            return this.impl.apply(object);
        }

    @Override
    public TypeInformation<K> getProducedType() {
        return this.typeInformation;
    }
}

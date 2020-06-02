package org.qcri.rheem.flink.compiler;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.qcri.rheem.core.function.TransformationDescriptor;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Wrapper for {@Link KeySelector}
 */
public class KeySelectorFunction<T, K> implements KeySelector<T, K>, ResultTypeQueryable<K>, Serializable {

    public Function<T, K> impl;

    public Class<K> key;

    public TypeInformation<K> typeInformation;

    public KeySelectorFunction(TransformationDescriptor<T, K> transformationDescriptor) {

        this.impl = transformationDescriptor.getJavaImplementation();
        this.key  = transformationDescriptor.getOutputType().getTypeClass();
        this.typeInformation = TypeInformation.of(this.key);
    }

    public K getKey(T object){
            return this.impl.apply(object);
        }

    @Override
    public TypeInformation getProducedType() {
        return this.typeInformation;
    }
}

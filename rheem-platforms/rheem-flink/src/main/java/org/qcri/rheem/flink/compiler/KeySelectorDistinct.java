package org.qcri.rheem.flink.compiler;

import org.apache.flink.api.java.functions.KeySelector;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;

/**
 * Wrapper for {@Link KeySelector}
 */
public class KeySelectorDistinct<T> implements KeySelector<T, String>, Serializable {

    public String getKey(T value){
        try {
            ByteArrayOutputStream b = new ByteArrayOutputStream();
            ObjectOutputStream objStream = new ObjectOutputStream(b);
            objStream.writeObject(value);
            return Base64.getEncoder().encodeToString(b.toByteArray());
        }finally {
            return "";
        }
    }

}
package org.apache.wayang.plugin.hackit.core.sniffer.clone;

import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;

public class BasicCloner<I> implements Cloner<I, byte[]> {
    @Override
    public byte[] clone(I input) {
        return SerializationUtils.serialize((Serializable) input);
    }
}

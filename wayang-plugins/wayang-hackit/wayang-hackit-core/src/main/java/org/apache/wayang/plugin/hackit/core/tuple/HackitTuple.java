package org.apache.wayang.plugin.hackit.core.tuple;

import org.apache.wayang.plugin.hackit.core.action.ActionGroup;
import org.apache.wayang.plugin.hackit.core.tags.HackitTag;
import org.apache.wayang.plugin.hackit.core.tuple.header.Header;
import org.apache.wayang.plugin.hackit.core.tuple.header.HeaderBuilder;

import java.io.Serializable;
import java.util.Iterator;

public class HackitTuple<K, T> implements Serializable, ActionGroup {
    private static HeaderBuilder BUILDER;
    private Header<K> header;
    private T value;

    static {
        BUILDER = new HeaderBuilder();
    }

    public HackitTuple(T value){
        this.header = BUILDER.generateHeader();
        this.value = value;
    }

    public HackitTuple(Header<K> header, T value){
        this.header = header;
        this.value = value;
    }

    public Header<K> getHeader(){
        return this.header;
    }

    public K getKey(){
        return this.header.getId();
    }

    public T getValue(){
        return this.value;
    }

    public void addTag(HackitTag tag){
        this.header.addTag(tag);
    }

    public Iterator<HackitTag> getTags(){
        return this.header.iterate();
    }

    @Override
    public String toString() {
        return "HackItTuple{" +
                "header=" + header +
                ", value=" + value +
                '}';
    }


    @Override
    public boolean hasCallback() {
        return this.getHeader().hasCallback();
    }

    @Override
    public boolean isHaltJob() {
        return this.getHeader().isHaltJob();
    }

    @Override
    public boolean isSendOut() {
        return this.getHeader().isSendOut();
    }

    @Override
    public boolean isSkip() {
        return this.getHeader().isSkip();
    }
}

package org.apache.wayang.plugin.hackit.core.tuple.header;

public class HeaderBuilder {


    public HeaderBuilder(){
        //TODO: take from the configuration
    }

    public <T> Header<T> generateHeader(){
        return (Header<T>) new HeaderLong();
    }

}

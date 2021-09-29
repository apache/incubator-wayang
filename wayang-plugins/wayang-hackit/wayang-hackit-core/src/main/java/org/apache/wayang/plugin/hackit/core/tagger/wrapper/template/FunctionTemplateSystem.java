package org.apache.wayang.plugin.hackit.core.tagger.wrapper.template;

public interface FunctionTemplateSystem<I, O> {

    public O execute(I input);
}

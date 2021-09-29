package org.apache.wayang.plugin.hackit.core.tagger.wrapper.template;

import java.util.Iterator;

public interface FlatMapTemplateSystem<I, O> {

    public Iterator<O> execute(I input);
}

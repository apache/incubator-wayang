package org.apache.wayang.plugin.hackit.core.sniffer.inject;

import org.apache.wayang.plugin.hackit.core.iterator.ElementPlusIterator;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

public class EmptyInjector<T> implements Injector<T>{

    @Override
    public Iterator<T> inject(T element, Iterator<T> iterator) {
        return
            StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(
                    new ElementPlusIterator<T>(
                        element,
                        iterator
                    ),
                    Spliterator.ORDERED
                ),
                false
            ).map(
                (T _element) -> {
                    if (this.is_halt_job(_element)) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    return _element;
                }
            ).filter(
                (T _element) -> ! this.is_skip_element(_element)
            ).iterator();
    }

    @Override
    public boolean is_skip_element(T element) {
        return false;
    }

    @Override
    public boolean is_halt_job(T element) {
        return false;
    }
}

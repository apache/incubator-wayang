package org.apache.wayang.plugin.hackit.core.iterator;

import java.util.Iterator;

public class ElementPlusIterator<T> implements Iterator<T> {

    private boolean element_consumed = false;
    private T element;
    private Iterator<T> iterator;


    public ElementPlusIterator(T element, Iterator<T> iterator) {
        if(element == null){
            throw new RuntimeException("the element can't be null");
        }
        this.element = element;
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        if( ! this.element_consumed ){
            return true;
        }
        return this.iterator.hasNext();
    }

    @Override
    public T next() {
        if( ! this.element_consumed ){
            this.element_consumed = true;
            return this.element;
        }
        return this.iterator.next();
    }
}

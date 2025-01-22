package org.example;

public class ComputeRequest<T> implements Message {
    private final T value;

    public ComputeRequest(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }
}
package org.example;

public class ComputeResponse<T> implements Message {
    private final T result;

    public ComputeResponse(T result) {
        this.result = result;
    }

    public T getResult() {
        return result;
    }
}
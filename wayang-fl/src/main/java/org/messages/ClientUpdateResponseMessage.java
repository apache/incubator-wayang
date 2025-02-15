package org.messages;

public class ClientUpdateResponseMessage<T> implements Message {
    private final T result;

    public ClientUpdateResponseMessage(T result) {
        this.result = result;
    }

    public T getResult() {
        return result;
    }
}
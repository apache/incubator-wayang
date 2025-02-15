package org.messages;

public class ClientUpdateRequestMessage<T> implements Message {
    private final T value;

    public ClientUpdateRequestMessage(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }
}
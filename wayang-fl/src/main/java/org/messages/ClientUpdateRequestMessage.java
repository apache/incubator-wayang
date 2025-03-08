package org.messages;

public class ClientUpdateRequestMessage implements Message {
    private final Object value;

    public ClientUpdateRequestMessage(Object value) {
        this.value = value;
    }

    public Object getValue() {
        return value;
    }
}
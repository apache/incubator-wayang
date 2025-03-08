package org.messages;

public class ClientUpdateResponseMessage implements Message {
    private final Object result;

    public ClientUpdateResponseMessage(Object result) {
        this.result = result;
    }

    public Object getResult() {
        return result;
    }
}
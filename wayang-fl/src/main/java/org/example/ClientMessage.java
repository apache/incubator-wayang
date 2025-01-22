package org.example;

public class ClientMessage implements Message {
    private final String clientId;
    private final String content;

    public ClientMessage(String clientId, String content) {
        this.clientId = clientId;
        this.content = content;
    }

    public String getClientId() {
        return clientId;
    }

    public String getContent() {
        return content;
    }
}

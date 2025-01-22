package org.example;

// Messages from Server
public class JoinAck implements Message {
    private final String clientId;

    public JoinAck(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }
}

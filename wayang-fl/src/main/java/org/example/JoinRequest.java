package org.example;

// Messages from Clients
public class JoinRequest implements Message {
    private final String clientId;

    public JoinRequest(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }
}

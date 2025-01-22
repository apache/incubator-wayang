package org.example;

public class LeaveAck implements Message {
    private final String clientId;

    public LeaveAck(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }
}

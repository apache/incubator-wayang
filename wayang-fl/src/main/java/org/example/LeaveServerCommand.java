package org.example;

public class LeaveServerCommand implements Message {
    private final String clientId;

    public LeaveServerCommand(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }
}
package org.example;

public class LeaveRequest implements Message {
    private final String clientId;

    public LeaveRequest(String clientId) {
        this.clientId = clientId;
    }

    public String getClientId() {
        return clientId;
    }
}

package org.messages;

import org.apache.pekko.actor.ActorSelection;
import org.client.Client;

import java.util.List;

public class InitiateHandshakeMessage implements Message{
    private final List<Client> clients;

    public InitiateHandshakeMessage(List<Client> clients){
        this.clients = clients;
    }

    public List<Client> getClients(){
        return clients;
    }
}

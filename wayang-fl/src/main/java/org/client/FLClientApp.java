package org.client;

import com.typesafe.config.Config;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;

public class FLClientApp {
    private final Client client;
    private final String platform_type;

    public FLClientApp(String client_url, String client_id, String platform_type){
        this.client = new Client(client_url, client_id);
        this.platform_type = platform_type;
    }

    public void startFLClient(Config config){
        ActorSystem system = ActorSystem.create(client.getName() + "-system", config);
        ActorRef FLClientActor = system.actorOf(
                Props.create(FLClient.class, () -> new FLClient(
                        client, platform_type
                )),
                client.getName()
        );
        System.out.println("Client is running");
    }
}

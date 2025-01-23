package org.example;

import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;

import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ClientApp {
    private static Plugin getPlugin(String platformType){
        if(platformType.equals("java")) return Java.basicPlugin();
        else if(platformType.equals("spark")) return Spark.basicPlugin();
        else return null;
    }
    public static void main(String[] args) throws Exception {
        ActorSystem system = ActorSystem.create("client-system");
        String clientId = "client-" + System.currentTimeMillis();  // Unique client ID
        String dataPath = args[0];
        String platformType = args[1];
        Plugin plugin = getPlugin(platformType);
        String serverPath = "pekko://server-system@127.0.0.1:2551/user/serverActor";  // Server's path

        ActorRef clientActor = system.actorOf(ClientActor.props(clientId, serverPath, dataPath, plugin), "clientActor");

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        String input;
        while ((input = reader.readLine()) != null) {
            if ("leave".equalsIgnoreCase(input)) {
                clientActor.tell(new LeaveServerCommand(clientId), ActorRef.noSender());
            }
            else {
                clientActor.tell(input, ActorRef.noSender());
            }
        }
    }
}

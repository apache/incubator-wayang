package org.client;


import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;

import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import org.functions.PlanFunction;
import org.messages.*;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

public class FLClient extends AbstractActor {

    private final Client client;
    private WayangPlan plan;
    private Collection<?> collector;
    private WayangContext wayangContext;
    private JavaPlanBuilder planBuilder;
    private PlanFunction planFunction;
    private Map<String, Object> hyperparams;

    private Plugin getPlugin(String platformType){
        if(platformType.equals("java")) return Java.basicPlugin();
        else if(platformType.equals("spark")) return Spark.basicPlugin();
        else return null;
    }

    public Props props(Client client, String platformType) {
        return Props.create(FLClient.class, () -> new FLClient(client, platformType));
    }

    public FLClient(Client client, String platformType) {
        this.client = client;
        this.wayangContext = new WayangContext(new Configuration()).withPlugin(getPlugin(platformType));
        this.planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("ClientActor")
                .withUdfJarOf(FLClient.class);
        this.collector = new LinkedList<>();
    }

//    @Override
//    public void preStart() {
////        ActorSelection server = getContext().actorSelection(serverPath);
////        server.tell(new JoinRequest(clientId), getSelf());
//    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(HandshakeMessage.class, this::handleHandshakeMessage)
                .match(PlanHyperparametersMessage.class, this::handlePlanHyperparametersMessage)
                .match(ClientUpdateRequestMessage.class, this::handleClientUpdateRequestMessage)
                .build();
    }

    private void handleHandshakeMessage(HandshakeMessage msg) {
        getSender().tell(new HandshakeAckMessage(), getSelf());
        System.out.println("sent handshake ack to server");
    }

    private void handlePlanHyperparametersMessage(PlanHyperparametersMessage msg) {
        planFunction = msg.getSerializedplan();
        hyperparams = msg.getHyperparams();
    }

    private void buildPlan(Object operand){
        Operator op = planFunction.apply(operand, planBuilder, hyperparams);
        Class classType = op.getOutput(0).getType().getDataUnitType().getTypeClass();
        LocalCallbackSink<?> sink = LocalCallbackSink.createCollectingSink(collector, classType);
        op.connectTo(0, sink, 0);
        plan = new WayangPlan(sink);
    }

    private void handleClientUpdateRequestMessage(ClientUpdateRequestMessage msg) {
        Object operand = msg.getValue();
        buildPlan(operand);
        wayangContext.execute(client.getName() + "-job", plan);
        getSender().tell(new ClientUpdateResponseMessage(new LinkedList<>(collector)), getSelf());
        collector.clear();
    }
}

package org.example;

import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.plan.wayangplan.Operator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServerActor extends AbstractActor {

    private Map<String, ActorRef> clients;
    private int epoch;
    private final int epochs;
    private int expectedResponses;
    private Object accumulatedResult;
    private final Object zero;
    private Object cur;
//    private final String serializedPlan;
    private final BiFunction<Object, Object> accumulator;
    private final TriFunction<Operator, List<Double>, JavaPlanBuilder> planFunction;
//  Add a hashmap key, function pairs and current values. Also add a function update_hashmap

    public static Props props(TriFunction<Operator, List<Double>, JavaPlanBuilder> planFunction, BiFunction<Object, Object> accumulator, int epochs, Object zero, Object init) {
        return Props.create(ServerActor.class, () -> new ServerActor(planFunction, accumulator, epochs, zero, init));
    }

    public ServerActor(TriFunction<Operator, List<Double>, JavaPlanBuilder> planFunction, BiFunction<Object, Object> accumulator, int epochs, Object zero, Object init) {
        this.planFunction = planFunction;
        this.zero = zero;
        this.accumulator = accumulator;
        this.epochs = epochs;
        this.epoch = 0;
        this.clients = new HashMap<>();
        this.cur = init;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinRequest.class, this::handleJoinRequest)
                .match(LeaveRequest.class, this::handleLeaveRequest)
                .match(ClientMessage.class, this::handleClientMessage)
                .matchEquals("start", msg -> startEpochs())
                .match(ComputeResponse.class, this::handleComputeResponse)
                .build();
    }

    private void sendPlan(){
        for (ActorRef client : clients.values()) {
            client.tell(new PlanMessage(planFunction), getSelf());
        }
    }

    private void runEpoch(){
//        System.out.println(epoch);
        System.out.println("Before epoch: " + epoch);
        System.out.println(cur);
        expectedResponses = clients.size();
        accumulatedResult = zero;
        for (ActorRef client : clients.values()) {
            client.tell(new ComputeRequest(cur), getSelf());
        }
    }

    private void startEpochs() {
        sendPlan();
        runEpoch();
    }

    private void endEpochs(){
        System.out.println("Final accumulated result:");
        System.out.println(cur);
        getContext().stop(getSelf());
    }

    private void handleComputeResponse(ComputeResponse<?> response) {
        accumulatedResult = accumulator.apply(accumulatedResult, response.getResult());
        expectedResponses--;
        if (expectedResponses == 0) {
            cur = accumulatedResult;
            epoch++;
            if(epoch < epochs) runEpoch();
            else endEpochs();
        }
    }

    private void handleJoinRequest(JoinRequest msg) {
        String clientId = msg.getClientId();
        ActorRef clientRef = getSender();
        clients.put(clientId, clientRef);
        System.out.println("Client joined: " + clientId);
        clientRef.tell(new JoinAck(clientId), getSelf());
    }

    private void handleLeaveRequest(LeaveRequest msg) {
        String clientId = msg.getClientId();
        clients.remove(clientId);
        System.out.println("Client left: " + clientId);
        getSender().tell(new LeaveAck(clientId), getSelf());
    }

    private void handleClientMessage(ClientMessage msg) {
        System.out.println("Received message from " + msg.getClientId() + ": " + msg.getContent());
    }

    public Map<String, ActorRef> getClients() {
        return clients;
    }
}

package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorSelection;
import org.apache.pekko.actor.Props;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.api.serialization.SerializationUtils;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.wayang.core.plugin.Plugin;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class ClientActor extends AbstractActor {

    private final String clientId;
    private final String serverPath;
    private final String dataPath;
    private WayangPlan plan;
    private Collection<?> collector;
    private WayangContext wayangContext;
    private JavaPlanBuilder planBuilder;
    private TriFunction<Operator, List<Double>, JavaPlanBuilder> planFunction;

    public static Props props(String clientId, String serverPath, String dataPath, Plugin plugin) {
        return Props.create(ClientActor.class, () -> new ClientActor(clientId, serverPath, dataPath, plugin));
    }

    public ClientActor(String clientId, String serverPath, String dataPath, Plugin plugin) {
        this.clientId = clientId;
        this.serverPath = serverPath;
        this.dataPath = dataPath;
        this.wayangContext = new WayangContext(new Configuration()).withPlugin(plugin);
        this.planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("ClientActor")
                .withUdfJarOf(ClientActor.class);
        this.collector = new LinkedList<>();
    }

    @Override
    public void preStart() {
        ActorSelection server = getContext().actorSelection(serverPath);
        server.tell(new JoinRequest(clientId), getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(JoinAck.class, this::handleJoinAck)
                .match(LeaveAck.class, this::handleLeaveAck)
                .match(LeaveServerCommand.class, this::leaveServer)
                .match(ComputeRequest.class, this::handleComputeRequest)
                .match(String.class, this::handleUserInput)
                .match(PlanMessage.class, this::handlePlanMessage)
                .build();
    }

    private void updateField(JsonNode node, String fieldName, String newValue) {
        if (node.isObject()) {
            ObjectNode objectNode = (ObjectNode) node;
            JsonNode field = objectNode.get(fieldName);

            if (field != null && field.isTextual()) {
                objectNode.put(fieldName, newValue);
            }
            objectNode.fields().forEachRemaining(entry -> updateField(entry.getValue(), fieldName, newValue));

        } else if (node.isArray()) {
            node.forEach(element -> updateField(element, fieldName, newValue));
        }
    }

    private void handlePlanMessage(PlanMessage planMessage) throws JsonProcessingException {
        planFunction = planMessage.getSerializedplan();
//        ObjectMapper mapper = new ObjectMapper();
//        JsonNode rootNode = mapper.readTree(serializedPlan);
//        updateField(rootNode, "inputUrl", dataPath);
//        serializedPlan = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
//        Operator op = SerializationUtils.deserializeFromString(serializedPlan, scala.reflect.ClassTag.apply(Operator.class));
//        Class classType = op.getOutput(0).getType().getDataUnitType().getTypeClass();
//        LocalCallbackSink<?> sink = LocalCallbackSink.createCollectingSink(collector, classType);
//        op.connectTo(0, sink, 0);
//        plan = new WayangPlan(sink);
    }

    private void buildPlan(List<Double> weights){
        Operator op = planFunction.apply(weights, planBuilder);
        Class classType = op.getOutput(0).getType().getDataUnitType().getTypeClass();
        LocalCallbackSink<?> sink = LocalCallbackSink.createCollectingSink(collector, classType);
        op.connectTo(0, sink, 0);
        plan = new WayangPlan(sink);
    }

    private void handleJoinAck(JoinAck msg) {
        System.out.println("Joined server as: " + msg.getClientId());
    }

    private void handleLeaveAck(LeaveAck msg) {
        System.out.println("Left server: " + msg.getClientId());
        getContext().stop(getSelf());
    }

    private void handleUserInput(String input) {
        if("start".equalsIgnoreCase(input)){
            System.out.println("Sent start cmd");
            getContext().actorSelection(serverPath).tell(input, getSelf());
        }
        else getContext().actorSelection(serverPath).tell(new ClientMessage(clientId, input), getSelf());
    }

    public void leaveServer(LeaveServerCommand msg) {
        getContext().actorSelection(serverPath).tell(new LeaveRequest(clientId), getSelf());
    }

    private void handleComputeRequest(ComputeRequest<?> msg) {
//        String jobName = (String) msg.getValue();
        List<Double> weights = (List<Double>) msg.getValue();
        buildPlan(weights);
        wayangContext.execute("CHALJAO", plan);
        getSender().tell(new ComputeResponse<>(new LinkedList<>(collector)), getSelf());
        collector.clear();
    }
}

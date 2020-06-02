package org.qcri.rheem.core.profiling;

import de.hpi.isg.profiledb.store.model.Measurement;
import de.hpi.isg.profiledb.store.model.Type;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This {@link Measurement} encapsulates an {@link ExecutionPlan}.
 */
@Type("execution-plan")
public class ExecutionPlanMeasurement extends Measurement {

    /**
     * Stores the {@link Channel}s of an {@link ExecutionPlan} as {@link ChannelNode}s.
     */
    private List<ChannelNode> channels;

    /**
     * Stores the {@link ExecutionOperator}s of an {@link ExecutionPlan} as {@link OperatorNode}s.
     */
    private List<OperatorNode> operators;

    /**
     * Stores the connections between {@link Channel}s and {@link ExecutionOperator}s of an {@link ExecutionPlan} as {@link Link}s.
     */
    private List<Link> links;

    /**
     * Deserialization constructor.
     */
    private ExecutionPlanMeasurement() {
    }

    private ExecutionPlanMeasurement(String id) {
        super(id);
    }

    /**
     * Creates a new instance.
     *
     * @param executionPlan that should be reflected in the new instance
     * @param id            ID for the new instance
     * @return the new instance
     */
    public static ExecutionPlanMeasurement capture(ExecutionPlan executionPlan, String id) {
        ExecutionPlanMeasurement instance = new ExecutionPlanMeasurement(id);

        // Collect the tasks of the plan.
        Set<ExecutionTask> executionTasks = executionPlan.collectAllTasks();

        // Initialize the new instance.
        instance.operators = new ArrayList<>(executionTasks.size());
        instance.channels = new ArrayList<>(executionTasks.size());
        instance.links = new ArrayList<>(executionTasks.size());

        // Keep track of already created ChannelNodes.
        Map<Channel, ChannelNode> channelNodeMap = new HashMap<>(executionTasks.size());

        // Go over the ExecutionTasks and create all ChannelNodes, OperatorNodes, and Links immediately.
        int nextNodeId = 0;
        for (ExecutionTask executionTask : executionTasks) {
            // Create the OperatorNode.
            ExecutionOperator operator = executionTask.getOperator();
            OperatorNode operatorNode = new OperatorNode(
                    nextNodeId++,
                    operator.getClass().getCanonicalName(),
                    operator.getName(),
                    operator.getPlatform().getName()
            );
            instance.operators.add(operatorNode);

            // Create inbound ChannelNodes and Links.
            for (Channel inputChannel : executionTask.getInputChannels()) {
                if (inputChannel == null) continue;

                ChannelNode channelNode = channelNodeMap.get(inputChannel);
                if (channelNode == null) {
                    channelNode = new ChannelNode(
                            nextNodeId++,
                            inputChannel.getClass().getCanonicalName(),
                            inputChannel.getDataSetType().getDataUnitType().getTypeClass().getName()
                    );
                    channelNodeMap.put(inputChannel, channelNode);
                    instance.channels.add(channelNode);
                }

                instance.links.add(new Link(channelNode.getId(), operatorNode.getId()));
            }

            // Create outbound ChannelNodes and Links.
            for (Channel outputChannel : executionTask.getOutputChannels()) {
                if (outputChannel == null) continue;

                ChannelNode channelNode = channelNodeMap.get(outputChannel);
                if (channelNode == null) {
                    channelNode = new ChannelNode(
                            nextNodeId++,
                            outputChannel.getClass().getCanonicalName(),
                            outputChannel.getDataSetType().getDataUnitType().getTypeClass().getName()
                    );
                    channelNodeMap.put(outputChannel, channelNode);
                    instance.channels.add(channelNode);
                }

                instance.links.add(new Link(operatorNode.getId(), channelNode.getId()));
            }
        }

        return instance;
    }

    public List<ChannelNode> getChannels() {
        return channels;
    }

    public List<OperatorNode> getOperators() {
        return operators;
    }

    public List<Link> getLinks() {
        return links;
    }

    /**
     * Encapsulates a {@link Channel} of the {@link ExecutionPlan}.
     */
    public static class ChannelNode {

        /**
         * ID of this instance to be used in {@link Link}s.
         */
        private int id;

        /**
         * The type of the {@link Channel}.
         */
        private String type;

        /**
         * The type of data quanta in the {@link Channel}.
         */
        private String dataQuantaType;

        /**
         * Deserialization constructor.
         */
        private ChannelNode() {
        }

        public ChannelNode(int id, String type, String dataQuantaType) {
            this.id = id;
            this.type = type;
            this.dataQuantaType = dataQuantaType;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getDataQuantaType() {
            return dataQuantaType;
        }

        public void setDataQuantaType(String dataQuantaType) {
            this.dataQuantaType = dataQuantaType;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }
    }

    /**
     * Encapsulates a {@link Channel} of the {@link ExecutionPlan}.
     */
    public static class OperatorNode {

        /**
         * ID of this instance to be used in {@link Link}s.
         */
        private int id;

        /**
         * The type of the {@link ExecutionOperator}.
         */
        private String type;

        /**
         * The name of the {@link ExecutionOperator}.
         */
        private String name;

        /**
         * The name of the {@link org.qcri.rheem.core.platform.Platform} of the {@link ExecutionOperator}.
         */
        private String platform;

        /**
         * Deserialization constructor.
         */
        private OperatorNode() {
        }

        public OperatorNode(int id, String type, String name, String platform) {
            this.id = id;
            this.type = type;
            this.name = name;
            this.platform = platform;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getPlatform() {
            return platform;
        }

        public void setPlatform(String platform) {
            this.platform = platform;
        }
    }

    /**
     * A directed link between an {@link OperatorNode} and a {@link ChannelNode} (in any order).
     */
    public static class Link {

        private int source, destination;

        /**
         * Deserialization constructor.
         */
        private Link() {
        }

        public Link(int source, int destination) {
            this.source = source;
            this.destination = destination;
        }

        public int getSource() {
            return source;
        }

        public void setSource(int source) {
            this.source = source;
        }

        public int getDestination() {
            return destination;
        }

        public void setDestination(int destination) {
            this.destination = destination;
        }
    }

}

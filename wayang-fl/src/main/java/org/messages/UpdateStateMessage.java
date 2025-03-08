package org.messages;


public class UpdateStateMessage implements Message{
    private final Object aggregatedResult;

    public UpdateStateMessage(Object aggregatedResult){
        this.aggregatedResult = aggregatedResult;
    }

    public Object getAggregatedResult(){
        return aggregatedResult;
    }
}

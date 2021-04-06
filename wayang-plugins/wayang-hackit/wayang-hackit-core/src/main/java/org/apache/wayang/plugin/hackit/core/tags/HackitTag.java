package org.apache.wayang.plugin.hackit.core.tags;

import org.apache.wayang.plugin.hackit.core.action.ActionGroup;
import org.apache.wayang.plugin.hackit.core.tagger.TaggerFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class HackitTag implements Serializable, ActionGroup {

    private List<TaggerFunction<?>> callbacks;
    private Map<String, Object> callback_results;

    protected HackitTag(){
        this.callbacks = new ArrayList<>();
        this.callback_results = new HashMap<>();
    }

    public void callback(){
        if(hasCallback()) {
            this.callbacks.stream().forEach(
                    fun -> this.addValue(fun.getName(), fun.execute())
            );
        }
    }
    public void addValue(String name, Object value){
        this.callback_results.put(name, value);
    }

    public abstract HackitTag getInstance();

    @Override
    public boolean equals(Object obj) {
        return this.getClass().equals(obj.getClass());
    }

    @Override
    public int hashCode() {
        return 0;
    }
}

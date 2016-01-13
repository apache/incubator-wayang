package org.qcri.rheem.core.plan;

import java.util.HashMap;
import java.util.Map;

/**
 * This mapping can be used to encapsulate subplans by connecting slots (usually <b>against</b> the data flow direction).
 */
public class SlotMapping {

    private final Map<Slot, Slot> mapping = new HashMap<>();

    public static SlotMapping wrap(Operator wrappee, Operator wrapper) {
        SlotMapping slotMapping = new SlotMapping();
        slotMapping.mapAll(wrapper.getAllOutputs(), wrappee.getAllOutputs());
        slotMapping.mapAll(wrappee.getAllInputs(), wrapper.getAllInputs());
        return slotMapping;
    }

    public void mapAll(InputSlot[] sources, InputSlot[] targets) {
        if (sources.length != targets.length) throw new IllegalArgumentException();
        for (int i = 0; i < sources.length; i++) {
            map(sources[i], targets[i]);
        }
    }

    public void mapAll(OutputSlot[] sources, OutputSlot[] targets) {
        if (sources.length != targets.length) throw new IllegalArgumentException();
        for (int i = 0; i < sources.length; i++) {
            map(sources[i], targets[i]);
        }
    }

    public void map(InputSlot<?> source, InputSlot<?> target) {
        if (!source.isCompatibleWith(target)) {
            throw new IllegalArgumentException("Incompatible slots given.");
        }

        this.mapping.put(source, target);
    }

    public void map(OutputSlot<?> source, OutputSlot<?> target) {
        if (!source.isCompatibleWith(target)) {
            throw new IllegalArgumentException("Incompatible slots given.");
        }

        this.mapping.put(source, target);
    }

    public <T> InputSlot<T> resolve(InputSlot<T> source) {
        return (InputSlot<T>) this.mapping.get(source);
    }

    public <T> OutputSlot<T> resolve(OutputSlot<T> source) {
        return (OutputSlot<T>) this.mapping.get(source);
    }

}

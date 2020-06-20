package org.qcri.rheem.core.test;

import org.json.JSONObject;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.util.JsonSerializable;

/**
 * Dummy {@link ExecutionOperator} for test purposes.
 */
public class SerializableDummyExecutionOperator extends DummyExecutionOperator implements JsonSerializable {

    public SerializableDummyExecutionOperator(int someProperty) {
        super(1, 1, false);
        this.setSomeProperty(someProperty);
    }

    @Override
    public JSONObject toJson() {
        return new JSONObject().put("someProperty", this.getSomeProperty());
    }

    @SuppressWarnings("unused")
    public static SerializableDummyExecutionOperator fromJson(JSONObject jsonObject) {
        return new SerializableDummyExecutionOperator(jsonObject.getInt("someProperty"));
    }
}

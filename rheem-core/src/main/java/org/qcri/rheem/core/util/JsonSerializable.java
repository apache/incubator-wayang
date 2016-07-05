package org.qcri.rheem.core.util;

import org.json.JSONObject;

/**
 * This interface prescribes implementing instances to be able to provide itself as a {@link JSONObject}.
 */
public interface JsonSerializable {

    /**
     * Convert this instance to a {@link JSONObject}.
     *
     * @return the {@link JSONObject}
     */
    JSONObject toJson();

}

package profiledb.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import profiledb.model.Measurement;

import java.lang.reflect.Type;

/**
 * Custom serializer for {@link Measurement}s
 * Detects actual subclass of given instances, encodes this class membership, and then delegates serialization to that subtype.
 */
public class MeasurementSerializer implements JsonSerializer<Measurement> {

    @Override
    public JsonElement serialize(Measurement measurement, Type type, JsonSerializationContext jsonSerializationContext) {
        final JsonObject jsonObject = (JsonObject) jsonSerializationContext.serialize(measurement);
        jsonObject.addProperty("type", measurement.getType());
        return jsonObject;
    }
}

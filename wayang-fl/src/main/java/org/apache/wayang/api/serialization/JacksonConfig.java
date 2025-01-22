package org.apache.wayang.api.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.wayang.api.serialization.customserializers.GenericSerializableSerializer;
import org.apache.wayang.api.serialization.customserializers.GenericSerializableDeserializer;

import org.apache.wayang.core.plan.wayangplan.WayangPlan;

public class JacksonConfig {
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        SimpleModule module = new SimpleModule();
        module.addSerializer(WayangPlan.class, new GenericSerializableSerializer<>());
        module.addDeserializer(WayangPlan.class, new GenericSerializableDeserializer<>());
        mapper.registerModule(module);
        mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
    }

    public static ObjectMapper getMapper() {
        return mapper;
    }
}

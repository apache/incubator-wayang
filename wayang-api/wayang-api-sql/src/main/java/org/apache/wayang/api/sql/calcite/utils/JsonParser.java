package org.apache.wayang.api.sql.calcite.utils;

import org.apache.wayang.core.api.Configuration;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

public class JsonParser {
    private Configuration configuration;
    private JSONObject json;

    public JsonParser() throws IOException, ParseException {
        Object obj = new JSONParser().parse(new FileReader("wayang-api/wayang-api-sql/src/main/resources/model.json"));
        this.json = (JSONObject) obj;
    }

    public JsonParser(Configuration configuration) throws IOException, ParseException {
        this.configuration = configuration;
        Object obj = new JSONParser().parse(new FileReader("wayang-api/wayang-api-sql/src/main/resources/model.json"));
        this.json = (JSONObject) obj;

    }

    public Configuration setProperties() {
        String calciteModel = json.toString();
        configuration.setProperty("wayang.calcite.model", calciteModel);

        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("postgres")) {
                JSONObject operand = (JSONObject) next.get("operand");
                configuration.setProperty("wayang.postgres.jdbc.url", operand.get("jdbcUrl").toString());
                configuration.setProperty("wayang.postgres.jdbc.user", operand.get("jdbcUser").toString());
                configuration.setProperty("wayang.postgres.jdbc.password", operand.get("jdbcPassword").toString());
            }
        }
        return configuration;
    }

    public String getFsPath() {
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("fs")) {
                JSONObject operand = (JSONObject) next.get("operand");
                return operand.get("directory").toString();
            }
        }
        return null;
    }
}

package org.apache.wayang.api.sql.calcite.utils;

import org.apache.wayang.core.api.Configuration;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

public class ModelParser {
    private Configuration configuration;
    private JSONObject json;

    public ModelParser() throws IOException, ParseException {
        Object obj = new JSONParser().parse(new FileReader("wayang-api/wayang-api-sql/src/main/resources/model.json"));
        this.json = (JSONObject) obj;
    }

    public ModelParser(Configuration configuration) throws IOException, ParseException {
        this.configuration = configuration;
        Object obj = new JSONParser().parse(new FileReader("wayang-api/wayang-api-sql/src/main/resources/model.json"));
        this.json = (JSONObject) obj;

    }

    public Configuration setProperties() {
        JSONObject calciteObj = (JSONObject) json.get("calcite");
        String calciteModel = calciteObj.toString();
        configuration.setProperty("wayang.calcite.model", calciteModel);

        JSONArray schemas = (JSONArray) calciteObj.get("schemas");

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
        JSONObject calciteObj = (JSONObject) json.get("calcite");
        JSONArray schemas = (JSONArray) calciteObj.get("schemas");

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

    public String getSeparator() {
        return (String) json.get("separator");
    }
}

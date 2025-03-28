/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        String calciteModel = "{\"calcite\"" + configuration.getStringProperty("wayang.calcite.model") + ",\"separator\":\";\"}";

        this.configuration = configuration;
        Object obj = new JSONParser().parse(calciteModel);
        System.out.println("obj: " + obj);
        this.json = (JSONObject) obj;
    }

    /**
     * This method allows you to specify the Calcite path, useful for testing.
     * See also {@link #ModelParser(Configuration)} and {@link #ModelParser()}.
     *
     * @param configuration    An empty configuration. Usage:
     *                         {@code Configuration configuration = new ModelParser(new Configuration(), calciteModelPath).setProperties();}
     * @param calciteModelPath Path to the JSON object containing the Calcite
     *                         model/schema.
     * @throws IOException    If an I/O error occurs.
     * @throws ParseException If unable to parse the file at
     *                        {@code calciteModelPath}.
     */
    public ModelParser(Configuration configuration, String calciteModelPath) throws IOException, ParseException {
        this.configuration = configuration;
        FileReader fr = new FileReader(calciteModelPath);
        Object obj = new JSONParser().parse(fr);

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

package org.components.hyperparameters;

import java.util.HashMap;
import java.util.Map;

public class Hyperparameters {
    private final Map<String, Object> server_hyperparams = new HashMap<>();
    private final Map<String, Object> client_hyperparams = new HashMap<>();

    public void update_server_hyperparams(String key, Object value){
        server_hyperparams.put(key, value);
    }

    public Object get_server_hyperparams(String key){
        return server_hyperparams.get(key);
    }

    public void update_client_hyperparams(String key, Object value){
        client_hyperparams.put(key, value);
    }

    public Object get_client_hyperparams(String key){
        return client_hyperparams.get(key);
    }

}

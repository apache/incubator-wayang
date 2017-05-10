package org.qcri.rheem.tests;

import org.json.JSONArray;
import org.json.JSONObject;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.RecordDinamic;

import java.util.Set;

public class ValidJSON {
    public static boolean comparateJSON(JSONObject json, RecordDinamic record, String ... token_words){
        Set<String> keys = json.keySet();
        for(String key: keys){
            Object element = json.get(key);
            boolean same_value;
            if(element instanceof JSONArray){
                same_value = compareJSON((JSONArray) element, (Record) record.getField(key), token_words);
            }else if(element instanceof JSONObject){
                same_value = comparateJSON((JSONObject) element, (RecordDinamic) record.getField(key), token_words);
            }else if(element instanceof String){
                same_value = ((String)element).equals(record.getString(key));
            }else if(element instanceof Integer){
                same_value = (Integer) element == ((Double)record.getDouble(key)).intValue();
            }else if(element instanceof Long) {
                same_value = (Long) element == ((Double) record.getDouble(key)).longValue();
                if ( ! same_value ){
                    long diff = ((Long) element - ((Double) record.getDouble(key)).longValue());
                    same_value = ( (diff < 2 && diff > -2) && (Math.log10(record.getDouble(key)) > 10))? true : false;
                }
            }else if(element instanceof Double) {
                same_value = ((Double) element).doubleValue() == ((Double)record.getDouble(key)).doubleValue();
            }else if(element == JSONObject.NULL) {
                same_value = record.getField(key) == null;
            }else if(element instanceof Boolean) {
                same_value = ((Boolean)element).booleanValue() == ((Boolean)record.getField(key)).booleanValue();
            }else{
                return false;
            }
            if( !same_value ){
                System.out.println(key);
                System.out.println(element);
                return false;
            }
        }
        return true;
    }

    private static boolean compareJSON(JSONArray json, Record record, String ... token_words){
        if(json.length() != record.size()){
            return false;
        }
        for(int i = 0; i < json.length(); i++){
            boolean same_value;
            Object element = json.get(i);
            if(element instanceof JSONArray){
                same_value = compareJSON((JSONArray) element, (Record) record.getField(i), token_words);
            }else if(element instanceof JSONObject){
                same_value = comparateJSON((JSONObject) element, (RecordDinamic) record.getField(i), token_words);
            }else if(element instanceof String){
                same_value = ((String)element).equals(record.getString(i));
            }else if(element instanceof Integer){
                same_value = (Integer) element == ((Double)record.getDouble(i)).intValue();
            }else if(element instanceof Long) {
                same_value = (Long) element == ((Double)record.getDouble(i)).longValue();
            }else if(element instanceof Double) {
                same_value = ((Double) element).doubleValue() == ((Double)record.getDouble(i)).doubleValue();
            }else if(element == JSONObject.NULL) {
                same_value = record.getField(i) == null;
            }else if(element instanceof Boolean) {
                same_value = ((Boolean)element).booleanValue() == ((Boolean)record.getField(i)).booleanValue();
            }else{
                return false;
            }
            if( !same_value ){
                return false;
            }
        }
        return true;
    }

    public static Object getkeyJSON(JSONObject json, String key_search){
        Set<String> keys = json.keySet();
        for(String key: keys){
            if(key_search.equals(key)){
                return json.get(key);
            }
            Object element = json.get(key);
            Object element_key = null;
            if(element instanceof JSONArray){
                element_key = getkeyJSON((JSONArray)element, key_search);
            }else if(element instanceof JSONObject){
                element_key = getkeyJSON((JSONObject)element, key_search);
            }
            if(element_key != null){
                return element_key;
            }
        }
        return null;
    }

    private static Object getkeyJSON(JSONArray json, String key_search){
        if(json.length() <= 0)
            return null;

        for(int i = 0; i < json.length(); i++){
            Object element = json.get(i);
            Object element_key = null;
            if(element instanceof JSONArray){
                element_key = getkeyJSON((JSONArray)element, key_search);
            }else if(element instanceof JSONObject){
                element_key = getkeyJSON((JSONObject)element, key_search);
            }
            if(element_key != null){
                return element_key;
            }
        }
        return null;
    }

    public static boolean compare(Object json, Object record){
        if(json instanceof JSONArray){
            return compareJSON((JSONArray) json, (Record) record);
        }else if(json instanceof JSONObject){
            return comparateJSON((JSONObject) json, (RecordDinamic) record);
        }else if(json instanceof String){
            return ((String)json).equals((String)record);
        }else if(json instanceof Integer){
            return (Integer) json == ((Double)record).intValue();
        }else if(json instanceof Long) {
            return (Long) json == ((Double)record).longValue();
        }else if(json instanceof Double) {
            return ((Double) json).doubleValue() == ((Double)record).doubleValue();
        }else if(json == JSONObject.NULL) {
            return record == null;
        }else if(json instanceof Boolean) {
            return ((Boolean)json).booleanValue() == ((Boolean)record).booleanValue();
        }else{
            return false;
        }
    }
}

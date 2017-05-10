package org.qcri.rheem.basic.util;

import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.RecordDinamic;
import org.qcri.rheem.basic.util.Exception.JSONParserException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

/**
 * Utility with utilize the {@Link JSONSource} for parsing of the string
 */
public class JSONParser implements Serializable {

    /**
     * Generate a {@Link RecordDinamic} that represent the tokens finded in parsing, in case of the tokens is null
     * the {@Link RecordDinamic} contains all tokens finded in string
     *
     * @param json is a string that is parsing in the process of the convertion
     * @param tokens are the words a will find in the string, when is finded all words the process is over
     * @return {@Link RecordDinamic} is the representation of the JSON with records
     */
    public static RecordDinamic execute(String json, String... tokens){
        RecordDinamic record = new RecordDinamic(tokens);

        char[] vector = json.toCharArray();

        Stack<Character> parenthesis = new Stack<>();
        List<String> alltokens;

        boolean   allJson        = false;
        int       tokenToProcess;
        boolean[] findeds;
        if(tokens != null && tokens.length > 0) {
            findeds        = new boolean[tokens.length];
            tokenToProcess = tokens.length;
            alltokens      = Arrays.asList(tokens);

        }else{
            allJson        = true;
            tokenToProcess = -1;
            alltokens      = null;
            findeds        = null;
        }

        int     size         = json.length();
        int     start        = -1;
        int     length       = -1;
        int     objectStart  = -1;
        int     objectlength = -1;
        int     arrayStart   = -1;
        int     arrayLength  = -1;
        int     numberStart  = -1;
        int     numberLength = -1;

        boolean inString     = false;
        boolean inPairkey    = true;
        boolean nextKey      = false;
        boolean inObject     = false;
        boolean inArray      = false;
        boolean inNumber     = false;
        boolean underOfKey   = false;
        String  key          = null;
        Object  value        = null;

        for(int i = 0; i < size; i++){
            if(value != null){
                if(value instanceof Class){
                    value = null;
                }
                record.setValue(key, value);
                value = null;
                if (!allJson) {
                    if (--tokenToProcess <= 0) {
                        record.setUse(i, size+1);
                        return record;
                    }
                }
            }
            if(nextKey){
                switch (vector[i]){
                    case '{':
                    case '[':
                        parenthesis.push(vector[i]);
                        break;
                    case '}':
                    case ']':
                        if(!parenthesis.empty())
                            parenthesis.pop();
                        break;
                    case ',':
                        if(parenthesis.empty()){
                            nextKey = false;
                        }
                        break;
                    case '"':
                        if(!parenthesis.empty()){
                            parenthesis.clear();
                            inString = true;
                            start = i + 1;
                            length = 0;
                            nextKey  = false;
                        }
                        break;
                }
                continue;
            }
            if(inObject){
                if(vector[i] == '}'){
                    parenthesis.pop();
                    if( parenthesis.empty() ) {
                        value = execute(String.valueOf(vector, objectStart, objectlength));
                        underOfKey = false;
                        inObject = false;
                    }
                }
                if(vector[i] == '{'){
                    parenthesis.push(vector[i]);
                }
                objectlength++;
                continue;
            }
            if(inArray){
                if(vector[i] == ']'){
                    parenthesis.pop();
                    if( parenthesis.empty() ) {
                        value = createArray(String.valueOf(vector, arrayStart, arrayLength));
                        underOfKey = false;
                        inArray = false;
                    }
                }
                if(vector[i] == '['){
                    parenthesis.push(vector[i]);
                }
                arrayLength++;
                continue;
            }
            if(inString) {
                if (vector[i] == '"' && vector[i - 1] != '\\') {
                    inString = false;
                    if (inPairkey) {
                        key = String.valueOf(vector, start, length);
                        if (!allJson) {
                            int index = alltokens.indexOf(key);
                            if (index == -1) {
                                nextKey = true;
                                continue;
                            } else {
                                if (findeds[index]) {
                                    try {
                                        throw new JSONParserException("the word is repeated in json file");
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        System.exit(-1);
                                    }
                                }
                                findeds[index] = true;
                                underOfKey = true;
                            }
                        } else {
                            underOfKey = true;
                        }
                        continue;
                    } else {
                        value = String.valueOf(vector, start, length);
                    }
                }
                length++;
                continue;
            }
            if(inNumber){
                switch (vector[i]){
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                    case 'e':
                    case 'E':
                    case '.':
                    case '+':
                    case '-':
                        numberLength++;
                        continue;
                    default:
                        value = Double.parseDouble( String.valueOf(vector, numberStart, numberLength) );
                        inNumber = false;
                        break;
                }
            }
            if( ! inPairkey ){
                String word = null;
                switch (vector[i]){
                    case ' ':
                    case '\t':
                    case '\n':
                        continue;
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                    case '-':
                        numberStart = i;
                        numberLength = 1;
                        inNumber = true;
                        inPairkey =true;
                        continue;
                    case 'f':
                    case 'F':
                        word = "false";
                        break;
                    case 't':
                    case 'T':
                        word = "true";
                        break;
                    case 'n':
                    case 'N':
                        word = "null";
                        break;
                }
                if(word != null) {
                    String wordInJson = String.copyValueOf(vector, i , word.length());
                    if (!word.equalsIgnoreCase(wordInJson)) {
                        try {
                            throw new JSONParserException("the word ("+word+") finded is not word reserved [true, false, null]");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    i += word.length() - 1;
                    switch (word) {
                        case "false":
                            value = false;
                            break;
                        case "true":
                            value = true;
                            break;
                        case "null":
                            value = JSONParser.class;
                            break;
                    }
                    inPairkey =true;
                    continue;
                }
            }
            if( ! inString ) {
                switch (vector[i]) {
                    case '{':
                        if( underOfKey ) {
                            inObject     = true;
                            objectStart  = i;
                            objectlength = 1;
                            parenthesis.push(vector[i]);
                        }
                        break;
                    case '[':
                        if( underOfKey ){
                            inArray     = true;
                            arrayStart  = i + 1;
                            arrayLength = 0;
                            parenthesis.push(vector[i]);
                        }
                        break;
                    case '"':
                        inString = true;
                        start = i + 1;
                        length = 0;
                        break;
                    case ':':
                        inPairkey = false;
                        break;
                    case ',':
                        inPairkey = true;
                        break;

                    case ' ':
                    case '\n':
                    case '\t':
                        continue;

                    default:
                        break;
                }
            }

        }
        record.setUse(size+1, size+1);
        return record;
    }


    private static Record createArray(String json){
        ArrayList<Object> values = new ArrayList<>();
        Stack<Character> parenthesis = new Stack<>();


        int sizeJson     = json.length();
        char[] vector    = json.toCharArray();
        int valueStart   = -1;
        int valueLength  = -1;
        boolean isObject = false;
        boolean isArray  = false;
        boolean isString = false;
        boolean isConst  = false;
        boolean isNumber = false;



        for(int i = 0; i < sizeJson; i++){
            if(isObject){
                if(vector[i] == '}'){
                    parenthesis.pop();
                    if( parenthesis.empty() ) {
                        values.add( execute(String.copyValueOf(vector, valueStart, valueLength+1)) );
                        isObject = false;
                        continue;
                    }
                }
                if(vector[i] == '{'){
                    parenthesis.push(vector[i]);
                }
                valueLength++;
                continue;
            }
            if(isArray){
                if(vector[i] == ']'){
                    parenthesis.pop();
                    if( parenthesis.empty() ) {
                        values.add( createArray( String.copyValueOf(vector, valueStart, valueLength) ) );
                        isArray = false;
                        continue;
                    }
                }
                if(vector[i] == '['){
                    parenthesis.push(vector[i]);
                }
                valueLength++;
                continue;
            }
            if(isString){
                if(vector[i] == '"'){
                    values.add( String.copyValueOf(vector, valueStart, valueLength) );
                    isString = false;
                    continue;
                }
                valueLength++;
                continue;
            }
            if(isNumber){
                switch (vector[i]){
                    case '0':
                    case '1':
                    case '2':
                    case '3':
                    case '4':
                    case '5':
                    case '6':
                    case '7':
                    case '8':
                    case '9':
                    case 'e':
                    case 'E':
                    case '.':
                    case '+':
                    case '-':
                        valueLength++;
                        continue;
                    default:
                        values.add( Double.parseDouble( String.copyValueOf(vector, valueStart, valueLength) ) );
                        isNumber = false;
                        break;
                }
            }
            if(isConst){
                String word = "";
                switch (vector[i-1]){
                    case 'f':
                    case 'F':
                        word = "false";
                        break;
                    case 't':
                    case 'T':
                        word = "true";
                        break;
                    case 'n':
                    case 'N':
                        word = "null";
                        break;
                }
                String wordInJson = String.copyValueOf(vector, i-1, word.length());
                if( ! word.equalsIgnoreCase(wordInJson)){
                    try {
                        throw new JSONParserException("the word ("+word+") finded is not word reserved [true, false, null]");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                i += word.length() - 1;
                switch (word) {
                    case "false":
                        values.add(false);
                        break;
                    case "true":
                        values.add(true);
                        break;
                    case "null":
                        values.add(null);
                        break;
                }
                isConst = false;
            }
            switch (vector[i]){
                case ' ':
                case '\t':
                case '\n':
                    continue;
                case '{':
                    isObject = true;
                    valueStart = i;
                    valueLength = 1;
                    parenthesis.push(vector[i]);
                    break;
                case '[':
                    isArray = true;
                    valueStart = i;
                    valueLength = 1;
                    parenthesis.push(vector[i]);
                    break;
                case '"':
                    isString = true;
                    valueStart = i+1;
                    valueLength = 0;
                    break;
                case '0':
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                case '-':
                    valueStart = i;
                    valueLength = 1;
                    isNumber = true;
                    break;
                case 'f':
                case 'F':
                case 't':
                case 'T':
                case 'n':
                case 'N':
                    isConst = true;
                    break;
            }
        }

        return new Record(values.toArray());
    }


}

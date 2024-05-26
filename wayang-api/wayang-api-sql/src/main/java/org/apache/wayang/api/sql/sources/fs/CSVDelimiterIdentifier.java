package org.apache.wayang.api.sql.sources.fs;

import java.util.HashMap;
import java.util.Map;

public class CSVDelimiterIdentifier {

    public static char identifyDelimiter(String data) {
        Map<Character, Integer> delimiterCounts = new HashMap<>();
        for (char c : new char[]{',', ';', '\t', '|', ' '}) {
            delimiterCounts.put(c, 0);
        }

        for (char c : data.toCharArray()) {
            if (delimiterCounts.containsKey(c)) {
                delimiterCounts.put(c, delimiterCounts.get(c) + 1);
            }
        }

        char maxDelimiter = ' ';
        int maxCount = 0;
        for (Map.Entry<Character, Integer> entry : delimiterCounts.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxDelimiter = entry.getKey();
                maxCount = entry.getValue();
            }
        }
        return maxDelimiter;
    }


}




package org.apache.wayang.hackit.sidecar.webservice;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ExecutorManager {

    private static ExecutorService ex;
    public static Map<UUID, Thread> threadsMap;
    public static Map<UUID, List<String>> processesLogs;

    public static void init(){
        ex = Executors.newFixedThreadPool(1);
        threadsMap = new HashMap<>();
        processesLogs = new HashMap<>();
    }

    public ExecutorManager(){

    }

    public static UUID addThread(ProcessBuilder processNotExecuted) {

        UUID key = UUID.randomUUID();
        List<String> logs = new ArrayList<>();

        processesLogs.put(key, logs);

        ex.submit(() -> {
            try {
                Process p = processNotExecuted.start();
                BufferedReader stdInput = new BufferedReader(new
                        InputStreamReader(p.getInputStream()));
                stdInput.lines()
                        .forEach(
                                line -> {
                                    logs.add(line);
                                }
                        );

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return key;
    }

    public static UUID addThread(List<ProcessBuilder> processesNotExecuted) {

        UUID key = UUID.randomUUID();
        List<String> logs = new ArrayList<>();

        processesLogs.put(key, logs);

        System.out.println("Submitting processes");
        ex.submit(() -> {
            try {

                BufferedReader stdInput;
                for (ProcessBuilder pb : processesNotExecuted) {
                    Process p = pb.start();
                    System.out.println("Executing process " + pb.toString());
                    stdInput = new BufferedReader(new
                            InputStreamReader(p.getInputStream()));

                    System.out.println("Log:");
                    stdInput.lines()
                            .forEach(
                                    line -> {
                                        logs.add(line);
                                        System.out.println(line);
                                    }
                            );
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        return key;
    }

    public static List<String> getProcessLogs(UUID pID){

        return ExecutorManager.processesLogs.get(pID);
    }

    public static List<String> getProcessLogs(UUID pID, Integer startFrom){

        List<String> logs = ExecutorManager.processesLogs.get(pID);
        List<String> ls = new ArrayList<>();

        Object[] values = logs.toArray();
        for (int i = startFrom - 1; i < values.length; i++) {
            ls.add(values[i].toString());
        }

        return ls;
    }

}

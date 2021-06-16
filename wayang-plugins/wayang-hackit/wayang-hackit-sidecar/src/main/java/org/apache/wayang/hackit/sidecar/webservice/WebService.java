package org.apache.wayang.hackit.sidecar.webservice;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.wayang.core.util.ReflectionUtils;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

@RestController
@RequestMapping("/sidecar")
public class WebService {

    @GetMapping("/debug/request/get")
    public HttpStatus requestGet(){

        return HttpStatus.ACCEPTED;
    }

    @GetMapping("/debug/request/status")
    public HttpStatus requestStatus(){

        return HttpStatus.ACCEPTED;
    }

    @GetMapping("/debug/request/kill")
    public HttpStatus requestKill(){

        return HttpStatus.ACCEPTED;
    }

    @GetMapping("/debug/start")
    public UUID serviceStart(){

        try {

            String path = Paths.get(".").toRealPath() + "/wayang-plugins/wayang-hackit/wayang-hackit-sidecar/src/main/resources/";

            ProcessBuilder builder_proc1 = new ProcessBuilder("kubectl", "create", "-f", path + "volume.yaml");
            ProcessBuilder builder_proc2 = new ProcessBuilder("kubectl", "create", "-f", path + "claim.yaml");
            ProcessBuilder builder_proc3 = new ProcessBuilder("kubectl", "create", "-f", path + "jobmanager_new.yaml");
            ProcessBuilder builder_proc4 = new ProcessBuilder("kubectl", "create", "-f", path + "jobmanager-service.yaml");
            ProcessBuilder builder_proc5 = new ProcessBuilder("kubectl", "create", "-f", path + "taskmanager_new.yaml");
            //ProcessBuilder builder_proc6 = new ProcessBuilder("kubectl", "create", "-f", path + "loadbalancer.yaml");

            ProcessBuilder builder_proc6 = new ProcessBuilder("kubectl", "create", "namespace", "kafka");

            ProcessBuilder builder_proc7 = new ProcessBuilder("kubectl", "create", "-f", "https://strimzi.io/install/latest?namespace=kafka", "-n", "kafka");

            ProcessBuilder builder_proc8 = new ProcessBuilder("kubectl", "apply", "-f", "https://strimzi.io/examples/latest/kafka/kafka-persistent-single.yaml", "-n", "kafka");

            ProcessBuilder builder_proc9 = new ProcessBuilder("kubectl", "wait", "kafka/my-cluster", "--for=condition=Ready", "--timeout=300s", "-n", "kafka");

            List<ProcessBuilder> processes = new ArrayList<>();
            processes.add(0, builder_proc1);
            processes.add(1, builder_proc2);
            processes.add(2, builder_proc3);
            processes.add(3, builder_proc4);
            processes.add(4, builder_proc5);
            //processes.add(5, builder_proc6);

            processes.add(5, builder_proc6);
            processes.add(6, builder_proc7);
            processes.add(7, builder_proc8);
            processes.add(8, builder_proc9);

            UUID ProcessID = ExecutorManager.addThread(processes);

            return ProcessID;

            /*
            System.out.println(Paths.get(".").toRealPath() + "/wayang-plugins/wayang-hackit/wayang-hackit-sidecar/src/main/resources");

            File f = new File(Paths.get(".").toRealPath() + "/wayang-plugins/wayang-hackit/wayang-hackit-sidecar/src/main/resources");
            for (String s: f.list()
                 ) {
                System.out.println(s);
            }*/

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @GetMapping("/debug/demo")
    public HttpStatus serviceDemo(){

        try {
            // LocalEnvironment
            ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
            DataSet<String> data = env.readTextFile("file:///Users/rodrigopardomeza/flink/count");

            data
                    .filter(new FilterFunction<String>() {
                        public boolean filter(String value) {
                            return value.startsWith("T");
                        }
                    })
                    .writeAsText("file:///Users/rodrigopardomeza/flink/output");

            JobExecutionResult res = env.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return HttpStatus.ACCEPTED;
    }

    @GetMapping("/debug/plan/run")
    public HttpStatus planRun(){

        try {
            // LocalEnvironment
            ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("127.0.0.1", 8081);

            //ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("127.0.0.1", 57261, getJars());
            //DataSet<String> data = env.readTextFile("file:///Users/rodrigopardomeza/flink/count");
            DataSet<String> data = env.readTextFile("/mnt/example/count");

            data
                    /*.filter(new FilterFunction<String>() {
                        public boolean filter(String value) {
                            return value.startsWith("T");
                        }
                    })*/
                    .writeAsText("/mnt/example/output_z");

            JobExecutionResult res = env.execute();

        } catch (Exception e) {
            e.printStackTrace();
        }

        return HttpStatus.ACCEPTED;
    }

    private String[] getJars(){
        List<String> jars = new ArrayList<>(5);
        List<Class> clazzs = Arrays.asList(new Class[]{this.getClass()});

        clazzs.stream().map(
                ReflectionUtils::getDeclaringJar
        ).filter(
                element -> element != null
        ).forEach(jars::add);

        return jars.toArray(new String[0]);
    }

    @GetMapping("/debug/stop")
    public UUID serviceStop(){

        try{
            String path = Paths.get(".").toRealPath() + "/wayang-plugins/wayang-hackit/wayang-hackit-sidecar/src/main/resources/";

            //ProcessBuilder builder_proc1 = new ProcessBuilder("kubectl", "delete", "-f", path + "loadbalancer.yaml");
            ProcessBuilder builder_proc1 = new ProcessBuilder("kubectl", "delete", "-f", path + "jobmanager-service.yaml");
            ProcessBuilder builder_proc2 = new ProcessBuilder("kubectl", "delete", "-f", path + "taskmanager_new.yaml");
            ProcessBuilder builder_proc3 = new ProcessBuilder("kubectl", "delete", "-f", path + "jobmanager_new.yaml");
            ProcessBuilder builder_proc4 = new ProcessBuilder("kubectl", "delete", "-f", path + "claim.yaml");
            ProcessBuilder builder_proc5 = new ProcessBuilder("kubectl", "delete", "-f", path + "volume.yaml");

            ProcessBuilder builder_proc6 = new ProcessBuilder("kubectl", "delete", "namespace", "kafka");

            ProcessBuilder builder_proc7 = new ProcessBuilder("kubectl", "delete", "-f", "'https://strimzi.io/install/latest?namespace=kafka'", "-n", "kafka");


            // For now execute: kubectl port-forward deployment/jobmanager 8081:8081

            List<ProcessBuilder> processes = new ArrayList<>();
            processes.add(0, builder_proc1);
            processes.add(1, builder_proc2);
            processes.add(2, builder_proc3);
            processes.add(3, builder_proc4);
            processes.add(4, builder_proc5);


            processes.add(5, builder_proc6);
            processes.add(6, builder_proc7);
            //processes.add(5, builder_proc6);

            UUID ProcessID = ExecutorManager.addThread(processes);

            return ProcessID;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    @PostMapping(path = "/debug/check")
    public String checkStatus(
            @RequestParam("key") String key,
            @RequestParam(value = "startfrom", required = false) Integer startFrom
    ){
        List<String> ls;

        if(startFrom == null)
            ls = ExecutorManager.getProcessLogs(UUID.fromString(key));
        else
            ls = ExecutorManager.getProcessLogs(UUID.fromString(key), startFrom);

        String res = "";
        for (String s: ls)
            res += s + "\n";

        return res;
    }
}

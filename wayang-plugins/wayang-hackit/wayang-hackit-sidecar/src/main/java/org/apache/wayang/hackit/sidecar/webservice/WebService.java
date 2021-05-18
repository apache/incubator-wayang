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

        ProcessBuilder builder_proc1 = new ProcessBuilder("kubectl", "create", "-f", "volume.yaml");
        ProcessBuilder builder_proc2 = new ProcessBuilder("kubectl", "create", "-f", "claim.yaml");
        ProcessBuilder builder_proc3 = new ProcessBuilder("kubectl", "create", "-f", "jobmanager_new.yaml");
        ProcessBuilder builder_proc4 = new ProcessBuilder("kubectl", "create", "-f", "jobmanager-service.yaml");
        ProcessBuilder builder_proc5 = new ProcessBuilder("kubectl", "create", "-f", "taskmanger_new.yaml");

        List<ProcessBuilder> processes = new ArrayList<>();
        processes.add(0, builder_proc1);
        processes.add(1, builder_proc2);
        processes.add(2, builder_proc3);
        processes.add(3, builder_proc4);
        processes.add(4, builder_proc5);

        UUID ProcessID = ExecutorManager.addThread(processes);

        return ProcessID;
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

        ProcessBuilder builder_proc1 = new ProcessBuilder("kubectl", "delete", "-f", "volume.yaml");
        ProcessBuilder builder_proc2 = new ProcessBuilder("kubectl", "delete", "-f", "claim.yaml");
        ProcessBuilder builder_proc3 = new ProcessBuilder("kubectl", "delete", "-f", "jobmanager_new.yaml");
        ProcessBuilder builder_proc4 = new ProcessBuilder("kubectl", "delete", "-f", "jobmanager-service.yaml");
        ProcessBuilder builder_proc5 = new ProcessBuilder("kubectl", "delete", "-f", "taskmanger_new.yaml");

        List<ProcessBuilder> processes = new ArrayList<>();
        processes.add(0, builder_proc1);
        processes.add(1, builder_proc2);
        processes.add(2, builder_proc3);
        processes.add(3, builder_proc4);
        processes.add(4, builder_proc5);

        UUID ProcessID = ExecutorManager.addThread(processes);

        return ProcessID;
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

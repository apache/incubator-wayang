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
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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

        ProcessBuilder builder_proc1 = new ProcessBuilder("kubectl", "create", "-f", "jobmanager.yaml");
        ProcessBuilder builder_proc2 = new ProcessBuilder("kubectl", "create", "-f", "jobmanager-service.yaml");
        ProcessBuilder builder_proc3 = new ProcessBuilder("kubectl", "create", "-f", "taskmanger.yaml");

        List<ProcessBuilder> processes = new ArrayList<>();
        processes.add(0, builder_proc1);
        processes.add(1, builder_proc2);
        processes.add(2, builder_proc3);

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

    @GetMapping("/debug/stop")
    public HttpStatus serviceStop(){

        return HttpStatus.ACCEPTED;
    }
}

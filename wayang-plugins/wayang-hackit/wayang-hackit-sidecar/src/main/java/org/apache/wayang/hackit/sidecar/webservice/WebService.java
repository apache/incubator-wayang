package org.apache.wayang.hackit.sidecar.webservice;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

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
    public HttpStatus serviceStart(){

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

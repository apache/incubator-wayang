package org.apache.wayang.api.rest.server.spring.general;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.stream.Stream;

@RestController
public class WayangController {

    @GetMapping("/")
    public String all(){
        System.out.println("detected!");

        try {

            FileInputStream inputStream = new FileInputStream("/Users/rodrigopardomeza/wayang/incubator-wayang/protobuf/message");
            Pywayangplan.WayangPlan plan = Pywayangplan.WayangPlan.parseFrom(inputStream);

            String sourcepath = plan.getPlan().getSource().getPath();
            URL url = new File(sourcepath).toURI().toURL();

            return(url.toString());

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "Not working";
    }
}

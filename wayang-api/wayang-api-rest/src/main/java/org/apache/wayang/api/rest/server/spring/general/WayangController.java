package org.apache.wayang.api.rest.server.spring.general;

import org.apache.wayang.basic.operators.TextFileSink;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.types.DataSetType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.stream.Stream;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import shapeless.TypeClass;

@RestController
public class WayangController {

    @GetMapping("/")
    public String all(){
        System.out.println("detected!");

        try {

            FileInputStream inputStream = new FileInputStream("/Users/rodrigopardomeza/wayang/incubator-wayang/protobuf/message");
            Pywayangplan.WayangPlan plan = Pywayangplan.WayangPlan.parseFrom(inputStream);

            WayangContext wc = buildContext(plan);
            WayangPlan wp = buildPlan(plan);

            wc.execute(wp);
            return("Works!");

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "Not working";
    }

    public static URI createUri(String resourcePath) {
        try {
            return Thread.currentThread().getClass().getResource(resourcePath).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }

    }

    private WayangContext buildContext(Pywayangplan.WayangPlan plan) {
        WayangContext ctx = new WayangContext();
            /*for(Pywayangplan.Context.Platform platform : plan.getContext().getPlatformsList()){
                if (platform.getNumber() == 0)
                    ctx.with(Java.basicPlugin());
                else if(platform.getNumber() == 1)
                    ctx.with(Spark.basicPlugin());
            }*/

        plan.getContext().getPlatformsList().forEach(platform -> {
            if (platform.getNumber() == 0)
                ctx.with(Java.basicPlugin());
            else if (platform.getNumber() == 1)
                ctx.with(Spark.basicPlugin());
        });

        return ctx;
    }

    private WayangPlan buildPlan(Pywayangplan.WayangPlan plan) {

        try {

            String sourcepath = plan.getPlan().getSource().getPath();
            URL url = new File(sourcepath).toURI().toURL();
            System.out.println("sourceurl");
            System.out.println(url.toString());
            TextFileSource textFileSource = new TextFileSource(url.toString());

            DataSetType d = null;
            Class t = null;
            switch (plan.getPlan().getOutput().getNumber()){
                case 0:
                    d = DataSetType.createDefault(String.class);
                    t = String.class;
                    break;
                case 1:
                    d = DataSetType.createDefault(Integer.class);
                    t = Integer.class;
                    break;
            }
            String sinkpath = plan.getPlan().getSink().getPath();
            URL sink_url = new File(sinkpath).toURI().toURL();
            System.out.println("sink");
            System.out.println(sink_url.toString());
            TextFileSink textFileSink = new TextFileSink(
                    sink_url.toString(),
                    t
            );

            textFileSource.connectTo(0, textFileSink, 0);
            return new WayangPlan(textFileSink);

        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        throw new WayangException("Unable to create Plan");
    }
}

package org.temp;

import org.apache.wayang.ml4all.abstraction.api.Transform;
import org.apache.wayang.ml4all.utils.StringUtil;

import java.util.List;

public class LibSVMTransform extends Transform<double[], String> {

    int features;

    public LibSVMTransform (int features) {
        this.features = features;
    }

    @Override
    public double[] transform(String line) {
        List<String> pointStr = StringUtil.split(line, ' ');
        double[] point = new double[features+1];
        point[0] = Double.parseDouble(pointStr.get(0));
        for (int i = 1; i < pointStr.size(); i++) {
            if (pointStr.get(i).equals("")) {
                continue;
            }
//                String kv[] = pointStr.get(i).split(":", 2);
//                point[Integer.parseInt(kv[0])] = Double.parseDouble(kv[1]);
            point[i] = Double.parseDouble(pointStr.get(i));
        }
        return point;
    }
}
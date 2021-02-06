/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.iejoin.operators.spark_helpers;


import org.apache.wayang.iejoin.data.Data;

import java.io.Serializable;
import java.util.Comparator;

public class revDataComparator implements Serializable, Comparator<Data> {

    private static final long serialVersionUID = 1L;

    boolean asc1;
    boolean asc2;
    boolean revRowID;

    public revDataComparator(boolean asc1, boolean asc2, boolean revRowID) {
        this.asc1 = asc1;
        this.asc2 = asc2;
        this.revRowID = revRowID;
    }

    public int compare(Data o1, Data o2) {
        int dff = 0;
        if (asc1) {
            dff = o1.compareRank(o2);
        } else {
            dff = o2.compareRank(o1);
        }
        if (dff == 0) {
            int dff2 = 0;
            if (asc2) {
                dff2 = o1.compareTo(o2);
            } else {
                dff2 = o2.compareTo(o1);
            }
            // third level of sorting
            if (dff2 == 0) {
                if ((o1.isPivot() && o2.isPivot())
                        || (!o1.isPivot() && !o2.isPivot())) {
                    if (!revRowID) {
                        return ((int) o1.getRowID() - (int) o2.getRowID());
                    } else {
                        return ((int) o2.getRowID() - (int) o1.getRowID());
                    }
                } else if (o1.isPivot() && !revRowID) {
                    if (asc1) {
                        return 1;
                    } else {
                        return -1;
                    }
                } else if (o2.isPivot() && !revRowID) {
                    if (!asc1) {
                        return -1;
                    } else {
                        return 1;
                    }
                } else {
                    if (asc1) {
                        return -1;
                    } else {
                        return 1;
                    }
                }
            }
            return dff2;
        }
        return dff;
    }
}

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

package org.apache.wayang.apps.tpch.data.q1;

import java.io.Serializable;

/**
 * Tuple that is returned by Query 1.
 */
public class ReturnTuple implements Serializable {

    public char L_RETURNFLAG;

    public char L_LINESTATUS;

    public double SUM_QTY;

    public double SUM_BASE_PRICE;

    public double SUM_DISC_PRICE;

    public double SUM_CHARGE;

    public double AVG_QTY;

    public double AVG_PRICE;

    public double AVG_DISC;

    public int COUNT_ORDER;

    public ReturnTuple() {
    }

    public ReturnTuple(char l_RETURNFLAG,
                       char l_LINESTATUS,
                       double SUM_QTY,
                       double SUM_BASE_PRICE,
                       double SUM_DISC_PRICE,
                       double SUM_CHARGE,
                       double AVG_QTY,
                       double AVG_PRICE,
                       double AVG_DISC,
                       int COUNT_ORDER) {
        this.L_RETURNFLAG = l_RETURNFLAG;
        this.L_LINESTATUS = l_LINESTATUS;
        this.SUM_QTY = SUM_QTY;
        this.SUM_BASE_PRICE = SUM_BASE_PRICE;
        this.SUM_DISC_PRICE = SUM_DISC_PRICE;
        this.SUM_CHARGE = SUM_CHARGE;
        this.AVG_QTY = AVG_QTY;
        this.AVG_PRICE = AVG_PRICE;
        this.AVG_DISC = AVG_DISC;
        this.COUNT_ORDER = COUNT_ORDER;
    }

    @Override
    public String toString() {
        return "ReturnTuple{" +
                "L_RETURNFLAG=" + this.L_RETURNFLAG +
                ", L_LINESTATUS=" + this.L_LINESTATUS +
                ", SUM_QTY=" + this.SUM_QTY +
                ", SUM_BASE_PRICE=" + this.SUM_BASE_PRICE +
                ", SUM_DISC_PRICE=" + this.SUM_DISC_PRICE +
                ", SUM_CHARGE=" + this.SUM_CHARGE +
                ", AVG_QTY=" + this.AVG_QTY +
                ", AVG_PRICE=" + this.AVG_PRICE +
                ", AVG_DISC=" + this.AVG_DISC +
                ", COUNT_ORDER=" + this.COUNT_ORDER +
                '}';
    }
}

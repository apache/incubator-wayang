/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql;

import org.apache.log4j.BasicConfigurator;
import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;

import java.util.Collection;
import java.util.Iterator;

public class SqlAPI {


//    public static void exampleFs() throws Exception {
//        Configuration configuration = new Configuration();
//        configuration.setProperty("wayang.fs.table.url", "/data/Projects/databloom/test-data/orders.csv");
//
//        SqlContext sqlContext = new SqlContext(configuration);
//
//        /*Collection<Record> result = sqlContext.executeSql("Select o_orderkey, o_totalprice from fs.orders where " +
//                "o_totalprice > 100");*/
//
////        Collection<Record> result = sqlContext.executeSql("Select o_orderkey, o_totalprice from fs.orders");
//
//        Collection<Record> result = sqlContext.executeSql("Select o_orderkey, o_totalprice from fs.orders where " +
//                "o_totalprice > 100000");
//
//
//        printResults(10, result);
//
//    }


    public static void examplePostgres() throws Exception {

        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/imdb");
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "postgres");

        SqlContext sqlContext = new SqlContext(configuration);

        /*Collection<Record> result = sqlContext.executeSql(
                "select c_name, c_acctbal \n"
                +" from postgres.customer"s
        );*/

        Collection<Record> result = sqlContext.executeSql(
                "select title, id \n"
                        //"select title, \"year\" \n"
                        +"from postgres.movie m \n"
                        //+"where \"year\" > 2000"
                        + "join postgres.movie_genre g \n"
                        + "on m.id = g.movieid"
        );

        /*Collection<Record> result = sqlContext.executeSql(
                "select c_name, c_acctbal \n"
                +"from  postgres.customer \n"
                +"where c_name = 'Customer#000000001'"
        );*/

        /*Collection<Record> result = sqlContext.executeSql(
                "select c_name, c_phone, c_acctbal, c_nationkey \n"
                +"from  postgres.customer \n"
                +"where c_nationkey = 15"
        );*/


        printResults(10, result);
    }





    public static void main(String... args) throws Exception {
        BasicConfigurator.configure();
        new SqlAPI().examplePostgres();
//        new SqlAPI().exampleFs();
    }


    private static void printResults(int n, Collection<Record> result) {
        // print up to n records
        int count = 0;
        Iterator<Record> iterator = result.iterator();
        while(iterator.hasNext() && count++ < n) {
            Record record = iterator.next();
            System.out.print( " | ");
            for(int i = 0; i < record.size(); i ++) {
                System.out.print(record.getField(i).toString() + " | ");
            }
            System.out.println("");
        }
    }
}

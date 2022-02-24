/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.wayang.api.sql.executor;

import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.Source;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Data output
 */
public class CustomEnumerator<E> implements Enumerator<E> {

    private E current;

    private BufferedReader br;

    public CustomEnumerator(Source source) {
      try {
        this.br = new BufferedReader(source.reader());
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    @Override
    public E current() {
      return current;
    }

    @Override
    public boolean moveNext() {
      try {
        String line = br.readLine();
        if(line == null){
          return false;
        }
        current = (E)new Object[]{line}; // If there are multiple columns, here are multiple values
      } catch (IOException e) {
        e.printStackTrace();
        return false;
      }
      return true;
    }

    /**
     * Anomalies go here
     * */
    @Override
    public void reset() {
      System.out.println("Reported a wrong brother, does not support this operation");
    }

    /**
     * InputStream stream is closed here
     * */
    @Override
    public void close() {

    }
}

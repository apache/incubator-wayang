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

package org.apache.wayang.giraph.Algorithm;

import org.apache.wayang.core.api.exception.WayangException;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;

/**
 * Parameters for Basic PageRank implementation.
 */
public class PageRankParameters {

    public enum PageRankEnum{
        ITERATION
    }
    private static final Queue<Integer> stack_iteration = new LinkedList<Integer>();

    public static boolean hasElement(PageRankEnum name){
        switch (name){
            case ITERATION:
                return !stack_iteration.isEmpty();
            default:
                throw new WayangException("Parameter for PageRank not exist");
        }
    }

    public static int getParameter(PageRankEnum name){
        switch (name){
            case ITERATION:
                return stack_iteration.peek();
            default:
                throw new WayangException("Parameter for PageRank not exist");
        }
    }

    public static void setParameter(PageRankEnum name, Integer value){
        switch (name){
            case ITERATION:
                stack_iteration.add(value);
                return;
            default:
                throw new WayangException("Parameter for PageRank not exist");
        }
    }

}

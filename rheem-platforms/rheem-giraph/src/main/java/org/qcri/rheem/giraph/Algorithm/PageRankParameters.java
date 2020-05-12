package org.qcri.rheem.giraph.Algorithm;

import org.qcri.rheem.core.api.exception.RheemException;

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
                throw new RheemException("Parameter for PageRank not exist");
        }
    }

    public static int getParameter(PageRankEnum name){
        switch (name){
            case ITERATION:
                return stack_iteration.peek();
            default:
                throw new RheemException("Parameter for PageRank not exist");
        }
    }

    public static void setParameter(PageRankEnum name, Integer value){
        switch (name){
            case ITERATION:
                stack_iteration.add(value);
                return;
            default:
                throw new RheemException("Parameter for PageRank not exist");
        }
    }

}

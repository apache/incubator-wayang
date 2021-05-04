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
package org.apache.wayang.plugin.hackit.core.tags;

/**
 * PauseTag is the {@link HackitTag} that identify the {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
 * where is need to pause all the process and wait for a signal that tell what to do, either kill the process or continues
 */
public class PauseTag extends HackitTag  {

    /**
     * seed is the element that allow to have small overhead in memory, is like use
     * singleton.
     */
    private static PauseTag seed = null;

    /**
     * default construct
     */
    private PauseTag(){
        super();
    }

    @Override
    public boolean isSendOut() {
        //TODO: set the correct value to this return
        return false;
    }

    @Override
    public boolean isSkip() {
        //TODO: set the correct value to this return
        return false;
    }

    @Override
    public boolean isHaltJob() {
        //TODO: set the correct value to this return
        return false;
    }

    @Override
    public boolean hasCallback() {
        //TODO: set the correct value to this return
        return false;
    }

    @Override
    public HackitTag getInstance() {
        if(seed == null){
            seed = new PauseTag();
        }
        return seed;
    }

    @Override
    public int hashCode() {
        return 5;
    }
}

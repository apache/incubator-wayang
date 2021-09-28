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
package org.apache.wayang.plugin.hackit.core.sniffer;

import org.apache.wayang.plugin.hackit.core.sniffer.actor.Actor;
import org.apache.wayang.plugin.hackit.core.sniffer.clone.Cloner;
import org.apache.wayang.plugin.hackit.core.sniffer.inject.Injector;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.Shipper;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender.Sender;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.Receiver;
import org.apache.wayang.plugin.hackit.core.sniffer.sniff.Sniff;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.io.Serializable;
import java.util.Iterator;
import java.util.function.Function;

/**
 * {@link HackitSniffer} is one of the main functions of Hackit, this function has the responsibility of executing the
 * logic of sniffing tuples and applying the logic of {@link #apply(HackitTuple)}
 *
 * @param <K> type of key that it handle the {@link org.apache.wayang.plugin.hackit.core.tuple.header.Header}
 * @param <T> type of element that is wrapped by {@link HackitTuple}
 * @param <SentType> Type of the element that will be sent out by {@link Sender}
 * @param <SenderObj> Object class of the implementation of {@link Sender}
 * @param <ReceiverObj>Object class of the implementation of {@link Receiver}
 */
public class
    HackitSniffer<
        K,
        T,
        SentType,
        SenderObj extends Sender<SentType>,
        ReceiverObj extends Receiver<HackitTuple<K,T>>
    >
    implements
        Function<
            HackitTuple<K, T>,
            Iterator<HackitTuple<K, T>>
        >,
        Serializable {

    /**
     * Indicates if it is the first execution or not, because some function will need that information
     * to get instantiated
     */
    private transient boolean not_first = false;

    /**
     * {@link Injector} instance that will be used by {@link HackitSniffer} as component
     */
    private Injector<HackitTuple<K, T>> hackItInjector;

    /**
     * {@link Actor} instance that will be used by {@link HackitSniffer} as component
     */
    private Actor<HackitTuple<K, T>> actorFunction;

    /**
     * {@link Shipper} instance that will be used by {@link HackitSniffer} as component
     */
    private Shipper<HackitTuple<K, T>, SentType, SenderObj, ReceiverObj> shipper;

    /**
     * {@link Sniff} instance that will be used by {@link HackitSniffer} as component
     */
    private Sniff<HackitTuple<K, T>> hackItSniff;

    /**
     * {@link Cloner} instance that will be used by {@link HackitSniffer} as component
     */
    private Cloner<HackitTuple<K, T>, SentType> hackItCloner;

    /**
     * Construct with the components as parameters
     * @param hackItInjector {@link Injector} instance that will be used by {@link HackitSniffer} as component
     * @param actorFunction {@link Actor} instance that will be used by {@link HackitSniffer} as component
     * @param shipper {@link Shipper} instance that will be used by {@link HackitSniffer} as component
     * @param hackItSniff {@link Sniff} instance that will be used by {@link HackitSniffer} as component
     * @param hackItCloner {@link Cloner} instance that will be used by {@link HackitSniffer} as component
     */
    //TODO: it may private, because need to be executed just at the creation moment
    public HackitSniffer(
            Injector<HackitTuple<K, T>> hackItInjector,
            Actor<HackitTuple<K, T>> actorFunction,
            Shipper<HackitTuple<K, T>, SentType, SenderObj, ReceiverObj> shipper,
            Sniff<HackitTuple<K, T>> hackItSniff,
            Cloner<HackitTuple<K, T>, SentType> hackItCloner
    ) {
        this.hackItInjector = hackItInjector;
        this.actorFunction = actorFunction;
        this.shipper = shipper;
        this.hackItSniff = hackItSniff;
        this.hackItCloner = hackItCloner;
        this.not_first = false;
    }

    /**
     * Default Construct, this get all the components from configuration files
     */
    public HackitSniffer() {
        //TODO this over configuration file
        this.not_first = false;
    }

    /**
     * Apply contains the logic that need to be executed at each tuple that is process by the main pipeline,
     * <ol>
     *     <li>If is the first exection the function perform the connection between the sidecar and the main pipeline</li>
     *     <li>Validate if the tuple need to be sniffed</li>
     *     <li>
     *         <ol>
     *             <li>validate if the element have the condition to be sent out</li>
     *             <li>The tuple is cloned </li>
     *             <li>The Tuple is sended out by publishing it</li>
     *         </ol>
     *     </li>
     *     <li>From the shipper it looks if exist new elements to be injected</li>
     * </ol>
     *
     * @param ktHackItTuple
     * @return
     */
    @Override
    public Iterator<HackitTuple<K, T>> apply(HackitTuple<K, T> ktHackItTuple) {
        if(!this.not_first){
            this.shipper.subscribeAsProducer();
            this.shipper.subscribeAsConsumer();
            this.not_first = true;
        }

        if(this.hackItSniff.sniff(ktHackItTuple)){
            if(this.actorFunction.is_sendout(ktHackItTuple)){
                this.shipper.publish(
                        this.hackItCloner.clone(ktHackItTuple)
                );
            }
        }
        Iterator<HackitTuple<K, T>> inyection = this.shipper.getNexts();

        return this.hackItInjector.inject(ktHackItTuple, inyection);
    }

    /**
     * Set {@link Injector} instance that will be used by {@link HackitSniffer} as component
     *
     * @param hackItInjector {@link Injector} instance
     * @return self instance of the {@link HackitSniffer}
     */
    public HackitSniffer<K, T, SentType, SenderObj, ReceiverObj> setHackItInjector(Injector<HackitTuple<K, T>> hackItInjector) {
        this.hackItInjector = hackItInjector;
        return this;
    }

    /**
     * Set {@link Actor} instance that will be used by {@link HackitSniffer} as component
     *
     * @param actorFunction {@link Actor} instance
     * @return self instance of the {@link HackitSniffer}
     */
    public HackitSniffer<K, T, SentType, SenderObj, ReceiverObj> setActorFunction(Actor<HackitTuple<K, T>> actorFunction) {
        this.actorFunction = actorFunction;
        return this;
    }

    /**
     * Set {@link Shipper} instance that will be used by {@link HackitSniffer} as component
     *
     * @param shipper {@link Shipper} instance
     * @return self instance of the {@link HackitSniffer}
     */
    public HackitSniffer<K, T, SentType, SenderObj, ReceiverObj> setShipper(Shipper<HackitTuple<K, T>, SentType, SenderObj, ReceiverObj> shipper) {
        this.shipper = shipper;
        return this;
    }

    /**
     * Set {@link Sniff} instance that will be used by {@link HackitSniffer} as component
     *
     * @param hackItSniff {@link Sniff} instance
     * @return self instance of the {@link HackitSniffer}
     */
    public HackitSniffer<K, T, SentType, SenderObj, ReceiverObj> setHackItSniff(Sniff<HackitTuple<K, T>> hackItSniff) {
        this.hackItSniff = hackItSniff;
        return this;
    }

    /**
     * Set {@link Cloner} instance that will be used by {@link HackitSniffer} as component
     *
     * @param hackItCloner {@link Cloner} instance
     * @return self instance of the {@link HackitSniffer}
     */
    public HackitSniffer<K, T, SentType, SenderObj, ReceiverObj> setHackItCloner(Cloner<HackitTuple<K, T>, SentType> hackItCloner) {
        this.hackItCloner = hackItCloner;
        return this;
    }

    @Override
    public String toString() {
        return String.format("HackItSniffer{\n first=%s, \n hackItInjector=%s, \n actorFunction=%s, " +
                        "\n shipper=%s, \n hackItSniff=%s, \n hackItCloner=%s\n}"
                , not_first, hackItInjector, actorFunction, shipper, hackItSniff, hackItCloner);
    }
}

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

public class HackitSniffer<K, T, SentType, SenderObj extends Sender<SentType>, ReceiverObj extends Receiver<HackitTuple<K,T>> > implements Function<HackitTuple<K, T>, Iterator<HackitTuple<K, T>>>, Serializable {

    private transient boolean not_first = false;
    private Injector<HackitTuple<K, T>> hackItInjector;

    private Actor<HackitTuple<K, T>> actorFunction;

    private Shipper<HackitTuple<K, T>, SentType, SenderObj, ReceiverObj> shipper;

    private Sniff<HackitTuple<K, T>> hackItSniff;
    private Cloner<HackitTuple<K, T>, SentType> hackItCloner;

    public HackitSniffer(Injector<HackitTuple<K, T>> hackItInjector, Actor<HackitTuple<K, T>> actorFunction, Shipper<HackitTuple<K, T>, SentType, SenderObj, ReceiverObj> shipper, Sniff<HackitTuple<K, T>> hackItSniff, Cloner<HackitTuple<K, T>, SentType> hackItCloner) {
        this.hackItInjector = hackItInjector;
        this.actorFunction = actorFunction;
        this.shipper = shipper;
        this.hackItSniff = hackItSniff;
        this.hackItCloner = hackItCloner;
        this.not_first = false;
    }

    public HackitSniffer() {
        //TODO this over configuration file
        this.not_first = false;
    }

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

    public HackitSniffer<K, T, SentType, SenderObj, ReceiverObj> setHackItInjector(Injector<HackitTuple<K, T>> hackItInjector) {
        this.hackItInjector = hackItInjector;
        return this;
    }

    public HackitSniffer<K, T, SentType, SenderObj, ReceiverObj> setActorFunction(Actor<HackitTuple<K, T>> actorFunction) {
        this.actorFunction = actorFunction;
        return this;
    }

    public HackitSniffer<K, T, SentType, SenderObj, ReceiverObj> setShipper(Shipper<HackitTuple<K, T>, SentType, SenderObj, ReceiverObj> shipper) {
        this.shipper = shipper;
        return this;
    }

    public HackitSniffer<K, T, SentType, SenderObj, ReceiverObj> setHackItSniff(Sniff<HackitTuple<K, T>> hackItSniff) {
        this.hackItSniff = hackItSniff;
        return this;
    }

    public HackitSniffer<K, T, SentType, SenderObj, ReceiverObj> setHackItCloner(Cloner<HackitTuple<K, T>, SentType> hackItCloner) {
        this.hackItCloner = hackItCloner;
        return this;
    }

    @Override
    public String toString() {
        return "HackItSniffer{" +
                "\nfirst=" + not_first +
                ",\n hackItInjector=" + hackItInjector +
                ",\n actorFunction=" + actorFunction +
                ",\n shipper=" + shipper +
                ",\n hackItSniff=" + hackItSniff +
                ",\n hackItCloner=" + hackItCloner +
                "\n}";
    }
}

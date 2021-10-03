package org.apache.wayang.plugin.hackit.core.sniffer;

import org.apache.wayang.plugin.hackit.core.sniffer.actor.Actor;
import org.apache.wayang.plugin.hackit.core.sniffer.clone.BasicCloner;
import org.apache.wayang.plugin.hackit.core.sniffer.inject.EmptyInjector;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.Shipper;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.EmptyReceiver;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.Receiver;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender.EmptySender;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender.Sender;
import org.apache.wayang.plugin.hackit.core.sniffer.sniff.CollectionTagsToSniff;
import org.apache.wayang.plugin.hackit.core.sniffer.sniff.SingleTagToSniff;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.junit.Before;
import org.junit.jupiter.api.Test;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

class HackitSnifferTest {

    @Test
    void apply() {
        HackitTuple<String, String> ht = new HackitTuple<>("test");
        HackitSniffer hs = new HackitSniffer<String, String, byte[],
                Sender<byte[]>, Receiver<HackitTuple<String, String>>>(
                new EmptyInjector<>(),
                null,
                new Shipper() {
                    @Override
                    protected Sender createSenderInstance() {
                        return new MockUpSender();
                    }

                    @Override
                    protected Receiver createReceiverInstance() {
                        return new MockUpReceiver();
                    }

                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Object next() {
                        return null;
                    }

                },
                new SingleTagToSniff(),
                new BasicCloner<>()
        );
        hs.apply(ht);
    }

    @Test
    void setHackItInjector() {
        HackitTuple<String, String> ht = new HackitTuple<>("test");
        HackitSniffer hs = new HackitSniffer<String, String, byte[],
                Sender<byte[]>, Receiver<HackitTuple<String, String>>>(
                new EmptyInjector<>(),
                null,
                new Shipper() {
                    @Override
                    protected Sender createSenderInstance() {
                        return new MockUpSender();
                    }

                    @Override
                    protected Receiver createReceiverInstance() {
                        return new MockUpReceiver();
                    }

                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Object next() {
                        return null;
                    }

                },
                new SingleTagToSniff(),
                new BasicCloner<>()
        );
        hs.setHackItInjector(new EmptyInjector<>());
    }

    @Test
    void setActorFunction() {
        HackitTuple<String, String> ht = new HackitTuple<>("test");
        HackitSniffer hs = new HackitSniffer<String, String, byte[],
                Sender<byte[]>, Receiver<HackitTuple<String, String>>>(
                new EmptyInjector<>(),
                null,
                new Shipper() {
                    @Override
                    protected Sender createSenderInstance() {
                        return new MockUpSender();
                    }

                    @Override
                    protected Receiver createReceiverInstance() {
                        return new MockUpReceiver();
                    }

                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Object next() {
                        return null;
                    }

                },
                new SingleTagToSniff(),
                new BasicCloner<>()
        );
        hs.setActorFunction(new Actor<HackitTuple<String, String>>() {
            @Override
            public boolean is_sendout(HackitTuple<String, String> value) {
                return false;
            }
        });
    }

    @Test
    void setShipper() {
        HackitTuple<String, String> ht = new HackitTuple<>("test");
        HackitSniffer hs = new HackitSniffer<String, String, byte[],
                Sender<byte[]>, Receiver<HackitTuple<String, String>>>(
                new EmptyInjector<>(),
                null,
                new Shipper() {
                    @Override
                    protected Sender createSenderInstance() {
                        return new MockUpSender();
                    }

                    @Override
                    protected Receiver createReceiverInstance() {
                        return new MockUpReceiver();
                    }

                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Object next() {
                        return null;
                    }

                },
                new SingleTagToSniff(),
                new BasicCloner<>()
        );
        hs.setShipper(new Shipper() {
            @Override
            protected Sender createSenderInstance() {
                return new MockUpSender();
            }

            @Override
            protected Receiver createReceiverInstance() {
                return new MockUpReceiver();
            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Object next() {
                return null;
            }

        });
    }

    @Test
    void setHackItSniff() {
        HackitTuple<String, String> ht = new HackitTuple<>("test");
        HackitSniffer hs = new HackitSniffer<String, String, byte[],
                Sender<byte[]>, Receiver<HackitTuple<String, String>>>(
                new EmptyInjector<>(),
                null,
                new Shipper() {
                    @Override
                    protected Sender createSenderInstance() {
                        return new MockUpSender();
                    }

                    @Override
                    protected Receiver createReceiverInstance() {
                        return new MockUpReceiver();
                    }

                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Object next() {
                        return null;
                    }

                },
                new SingleTagToSniff(),
                new BasicCloner<>()
        );
        hs.setHackItSniff(new CollectionTagsToSniff());
    }

    @Test
    void setHackItCloner() {
        HackitTuple<String, String> ht = new HackitTuple<>("test");
        HackitSniffer hs = new HackitSniffer<String, String, byte[],
                Sender<byte[]>, Receiver<HackitTuple<String, String>>>(
                new EmptyInjector<>(),
                null,
                new Shipper() {
                    @Override
                    protected Sender createSenderInstance() {
                        return new MockUpSender();
                    }

                    @Override
                    protected Receiver createReceiverInstance() {
                        return new MockUpReceiver();
                    }

                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public Object next() {
                        return null;
                    }

                },
                new SingleTagToSniff(),
                new BasicCloner<>()
        );
        hs.setHackItCloner(new BasicCloner<>());
    }

    class MockUpSender<T> implements Sender<T>{

        @Override
        public void init() {

        }

        @Override
        public void send(T value) {

        }

        @Override
        public void close() {

        }
    }

    class MockUpReceiver<T> extends Receiver<T> {

        @Override
        public void init() {

        }

        @Override
        public Iterator<T> getElements() {
            return null;
        }

        @Override
        public void close() {

        }
    }

}
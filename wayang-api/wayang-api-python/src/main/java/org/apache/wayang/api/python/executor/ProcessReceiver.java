package org.apache.wayang.api.python.executor;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;

public class ProcessReceiver<Output> {

    private ReaderIterator<Output> iterator;

    public ProcessReceiver(Socket socket){
        try{
            //TODO use config buffer size
            int BUFFER_SIZE = 8192;

            DataInputStream stream = new DataInputStream(new BufferedInputStream(socket.getInputStream(), BUFFER_SIZE));
            this.iterator = new ReaderIterator<>(stream);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Iterable<Output> getIterable(){
        return () -> iterator;
    }

    public void print(){
        iterator.forEachRemaining(x -> System.out.println(x.toString()));

    }
}

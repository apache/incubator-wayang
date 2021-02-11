package org.apache.wayang.api.python.executor;

import org.apache.wayang.api.python.function.PythonUdf;
import org.apache.wayang.core.api.exception.WayangException;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

public class ProcessFeeder<Input, Output> {

    private Socket socket;
    private PythonUdf<Input, Output> udf;
    private Iterable<Input> input;

    //TODO add to a config file
    int END_OF_DATA_SECTION = -1;

    public ProcessFeeder(
            Socket socket,
            PythonUdf<Input, Output> udf,
            Iterable<Input> input){

        if(input == null) throw new WayangException("Nothing to process with Python API");

        this.socket = socket;
        this.udf = udf;
        this.input = input;

    }

    public void send(){

        try{
            BufferedOutputStream stream = new BufferedOutputStream(socket.getOutputStream(), 8192);
            DataOutputStream dataOut = new DataOutputStream(stream);

            this.writeIteratorToStream(input.iterator(), dataOut);
            dataOut.writeInt(END_OF_DATA_SECTION);
            dataOut.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeIteratorToStream(Iterator iter, DataOutputStream dataOut){

        for (Iterator it = iter; it.hasNext(); ) {
            Object elem = it.next();
            write(elem, dataOut);
        }
    }

    /*TODO Missing cases PortableDataStream, and NULL */
    public static void write(Object obj, DataOutputStream dataOut){
        try {

            /**
             * Byte cases
             */
            if (obj instanceof Byte[]) {

                int length = ((Byte[]) obj).length;

                byte[] bytes = new byte[length];
                int j=0;
                // Unboxing Byte values. (Byte[] to byte[])
                for(Byte b: ((Byte[]) obj))
                    bytes[j++] = b.byteValue();

                dataOut.writeInt(length);
                dataOut.write(bytes);

            } else if (obj instanceof byte[]) {

                dataOut.writeInt(((byte[]) obj).length);
                dataOut.write(((byte[]) obj));
            }

            /**
             * String case
             * */
            else if (obj instanceof String) {

                writeUTF((String) obj, dataOut);
            }

            /**
             * Key, Value Case case
             * */
            else if (obj instanceof Map.Entry) {

                write(((Map.Entry) obj).getKey() , dataOut);
                write(((Map.Entry) obj).getValue(), dataOut);
            } else{
                throw new WayangException("Unexpected element type " + obj.getClass());
            }


        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeUTF(String str, DataOutputStream dataOut){
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);

        try {
            dataOut.writeInt(bytes.length);
            dataOut.write(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

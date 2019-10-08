package applications.bolts.nexmark;

import java.io.Serializable;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import applications.util.datatypes.StreamValues;
import applications.datatype.util.ISegmentIdentifier;

public class NexmarkTuple extends StreamValues {
    private static final long serialVersionUID = 2L;

    // private byte[] buffer = new byte[32];

    public long auction;
    public long bidder;
    public long price;
    public long dateTime;

    public NexmarkTuple() {
        System.out.println("Default constructor of NexmarkTuple.");
    }

    public NexmarkTuple(char[] string) {
        ByteBuffer bufferHelper;
        byte[] buffer = new byte[32];
        for (int i = 0; i < 32; i ++) {
            // System.out.print(String.format("%02x", (byte)string[i]) + " ");
            buffer[i] = (byte) string[i];;
        }
        // System.out.println();

        bufferHelper = ByteBuffer.wrap(buffer);
        bufferHelper.order(ByteOrder.LITTLE_ENDIAN);
        bufferHelper.position(0);
        this.auction = bufferHelper.getLong();
        this.bidder = bufferHelper.getLong();
        this.price = bufferHelper.getLong();
        this.dateTime = bufferHelper.getLong();

        // System.out.println("[DBG]: auction: " + auction + ", bidder: " + bidder + ", price: " + price + ", dateTime: " + dateTime);
    }
}

package applications.bolts.ysb.model;

import java.io.Serializable;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.util.List;
import applications.util.datatypes.StreamValues;
import applications.datatype.util.ISegmentIdentifier;

public class YSBTuple extends StreamValues {
    private static final long serialVersionUID = 2L;

    public String userId;       // lsb + msb, 16 bytes
    public String pageId;       // lsb + msb, 16 bytes
    public String campaignId;   // lsb + msb, 16 bytes
    public String adType;
    public String eventType;
    public long eventTime;
    public int ipAddress;

    public YSBTuple(String string) {

    }

    public YSBTuple(char[] string) {
        char[] _userId = new char[16];
        char[] _pageId = new char[16];
        char[] _campaignId = new char[16];
        char[] _adType = new char[9];
        char[] _eventType = new char[9];
        char[] _eventTime = new char[8];
        char[] _ipAddress = new char[4];
        System.arraycopy(string, 0, _userId, 0, 16);
        System.arraycopy(string, 16, _pageId, 0, 16);
        System.arraycopy(string, 32, _campaignId, 0, 16);
        System.arraycopy(string, 48, _adType, 0, 9);
        System.arraycopy(string, 57, _eventType, 0, 9);
        System.arraycopy(string, 66, _eventTime, 0, 8);
        System.arraycopy(string, 74, _ipAddress, 0, 4);

        StringBuilder sb = null;
        sb = new StringBuilder();
        for (int i = 0; i < 16; i ++) {
            sb.append((int) _userId[i]);
        }
        this.userId = sb.toString();

        sb = new StringBuilder();
        for (int i = 0; i < 16; i ++) {
            sb.append((char) _pageId[i]);
        }
        this.pageId = sb.toString();

        sb = new StringBuilder();
        for (int i = 0; i < 16; i ++) {
            // System.out.print((short) _campaignId[i] + " ");
            sb.append((int) _campaignId[i]);
        }
        // System.out.println();
        this.campaignId = sb.toString();

        // byte[] arr = new byte[8];
        // for (int i = 0; i < 8; i ++) {
        //     arr[i] = (byte) _campaignId[i];
        // }
        // ByteBuffer wrapped = ByteBuffer.wrap(arr); // big-endian by default
        // this.campaignId = Long.toString(wrapped.getLong());

        this.adType = new String(_adType);
        this.eventType = new String(_eventType);

        // System.out.println("userId: " + userId + ", eventType: " + eventType + ", campaignId: " + campaignId);
        super.add(0, userId);
        super.add(1, pageId);
        super.add(2, campaignId);
        super.add(3, adType);
        super.add(4, eventType);
        // super.add(5, eventTime);
        // super.add(6, ipAddress);
    }

    public YSBTuple(int x) {
        this.userId = Integer.toString(x);
    }
}

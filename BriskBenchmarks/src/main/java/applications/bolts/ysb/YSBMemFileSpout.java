package applications.bolts.ysb;

import applications.constants.BaseConstants;
import applications.spout.helper.wrapper.StringStatesWrapper;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.context.TopologyContext;
import brisk.components.operators.api.AbstractSpout;
import brisk.execution.ExecutionGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.ArrayList;
import java.util.Scanner;


public class YSBMemFileSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(YSBMemFileSpout.class);
    private static final long serialVersionUID = -2394340130331865581L;
    protected ArrayList<char[]> array;
    protected int element = 0;
    protected int counter = 0;
    //	String[] array_array;
    char[][] array_array;
    // char[][] tuples;

    // byte[][] rawTuples;
    char[][] rawTuples;

    private transient BufferedWriter writer;
    private int cnt;
    private int taskId;
    private char [] tuple;
    private int currentIndex = 0;
    private int tupleToRead;
    private int tupleSize;

    public YSBMemFileSpout() {
        super(LOG);
        this.scalable = false;
    }

    @Override
    public Integer default_scale(Configuration conf) {

        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 2;
        } else {
            return 1;
        }
    }

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
    public String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        LOG.info("Spout initialize is being called");
        this.tupleToRead = 100;
        this.tupleSize = 78;
        this.currentIndex = 0;


        try {
            byte[][] _rawTuples = new byte[tupleToRead][tupleSize];
            String inputFile = "/home/zxchen/inputdata.bin";
            BufferedInputStream in = new BufferedInputStream(new FileInputStream(inputFile));
            int flag = -1;
            for (int i = 0; i < tupleToRead; i ++) {
                flag = in.read(_rawTuples[i], 0, tupleSize);
                if (flag == -1)
                    break;
            }
            // System.out.println("tuples[0]: " + bytesToHex(this.rawTuples[0]));
            // this.rawTuples = new char[tupleToRead][tupleSize];
            // BufferedReader in = new BufferedReader(new FileReader(inputFile));
            // int flag = -1;
            // for (int i = 0; i < tupleToRead; i ++) {
            //     flag = in.read(this.rawTuples[i], 0, tupleSize);
            //     if (flag == -1)
            //         break;
            // }
            // for (int j = 0; j < tupleSize; j ++) {
            //     char ch = this.rawTuples[0][j];
            //     String hex = String.format("%04x", (int) ch);
            //     System.out.print(hex + ' ');
            // }
            // System.out.println();

            // Covert to char array since BriskStream doesn't support byte array
            this.rawTuples = new char[tupleToRead][tupleSize];
            for (int i = 0; i < tupleToRead; i ++) {
                for (int j = 0; j < tupleSize; j ++) {
                    rawTuples[i][j] = (char)_rawTuples[i][j];
                }
            }

            for (int j = 0; j < tupleSize; j ++) {
                char ch = this.rawTuples[2][j];
                String hex = String.format("%02x", (byte)ch);
                System.out.print(hex + " ");
            }

            System.out.println();

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * relax_reset source messages.
     */
    @Override
    public void cleanup() {

    }

    @Override
    public void nextTuple() throws InterruptedException {
        collector.emit(this.rawTuples[this.currentIndex++]);
        if (this.currentIndex >= this.tupleToRead) {
            this.currentIndex = 0;
        }
    }

    public void display() {
        LOG.info("timestamp_counter:" + counter);
    }

}

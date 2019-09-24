package applications.bolts.nexmark;

import applications.constants.BaseConstants;
import applications.constants.WordCountConstants.Field;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.context.TopologyContext;
import brisk.components.operators.base.splitBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import brisk.components.operators.api.BaseOperator;
import brisk.execution.runtime.tuple.impl.OutputFieldsDeclarer;

import java.util.HashMap;

import applications.bolts.nexmark.NexmarkTuple;


public class NexmarkParser extends splitBolt {
    private static final Logger LOG = LoggerFactory.getLogger(NexmarkParser.class);
    private static final long serialVersionUID = 8089145995668583749L;

    public NexmarkParser () {
        super(LOG, new HashMap<>());
        this.output_selectivity.put(BaseConstants.BaseStream.DEFAULT, 10.0);
        OsUtils.configLOG(LOG);
    }

    public Integer default_scale(Configuration conf) {

        int numNodes = conf.getInt("num_socket", 1);
        if (numNodes == 8) {
            return 10;
        } else {
            return 1;
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
    }


    @Override
    public Fields getDefaultFields() {
        System.out.println("[DBG] NexmarkParser get Default Fields");
        return new Fields("campaign_id");
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        // System.out.println("[DBG] Execute NexmarkParser Tuple");
//		String value_list = in.getString(0);
//		String[] split = value_list.split(",");
//		for (String word : split) {
//			collector.force_emit(word);
//		}
        // char[] value = in.getCharArray(0);
        // int index = 0;
        // int length = value.length;
        // for (int c = 0; c < length; c++) {
        //     if (value[c] == ',' || c == length - 1) {//double measure_end.
        //         int len = c - index;
        //         char[] word = new char[len];
        //         System.arraycopy(value, index, word, 0, len);
        //         collector.force_emit(word);
        //         index = c + 1;
        //     }
        // }
    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;
        // System.out.println("[DBG] Execute NexmarkParser TransferTuple length: " + bound);
        for (int i = 0; i < bound; i++) {
            char[] raw = in.getCharArray(0, i);
            // char[] emit = this.parse(raw);
            // for (int j = 0; j < 78; j ++) {
            //     char ch = raw[j];
            //     String hex = String.format("%02x", (byte)ch);
            //     System.out.print(hex + " ");
            // }

            // System.out.println();
            collector.emit(0, new NexmarkTuple(raw));
            // collector.emit(0, new NexmarkTuple());
        }
    }

    public void profile_execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {

//			char[] value_list = in.getCharArray(0, i);
//			int index = 0;
//			int length = value_list.length;
//			for (int c = 0; c < length; c++) {
//				if (value_list[c] == ',' || c == length - 1) {//double measure_end.
//					int len = c - index;
//					char[] word = new char[len];
//					System.arraycopy(value_list, index, word, 0, len);
//					collector.emit_nowait(word);
//					index = c + 1;
//				}
//			}

            // char[] value = in.getCharArray(0, i);
            // String[] split = new String(value).split(",");
            // for (String word : split) {
            //     collector.emit_nowait(word.toCharArray());
            // }
            char[] raw = in.getCharArray(0, i);
            collector.emit_nowait(new NexmarkTuple(raw));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("auction", "bidder", "price", "dateTime"));
    }

}

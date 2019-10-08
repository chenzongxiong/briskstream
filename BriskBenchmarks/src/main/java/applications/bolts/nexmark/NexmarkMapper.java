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


public class NexmarkMapper extends splitBolt {
    private static final Logger LOG = LoggerFactory.getLogger(NexmarkMapper.class);
    private static final long serialVersionUID = 8089145995668583749L;

    public NexmarkMapper () {
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
        System.out.println("[DBG] NexmarkMapper get Default Fields");
        return new Fields("campaign_id");
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        // System.out.println("[DBG] Execute NexmarkMapper Tuple");
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
        // System.out.println("[DBG] Execute NexmarkMapper TransferTuple length: " + bound);
        for (int i = 0; i < bound; i++) {
            NexmarkTuple tuple = (NexmarkTuple) in.getValue(0, i);
            tuple.price = (long)(tuple.price * 0.89);
            collector.emit(0, tuple);
        }
    }

    public void profile_execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            NexmarkTuple tuple = (NexmarkTuple) in.getValue(0, i);
            tuple.price = (long)(tuple.price * 0.89);
            collector.emit_nowait(0, tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("auction", "bidder", "price", "dateTime"));
    }

}

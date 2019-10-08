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
import applications.bolts.nexmark.NexmarkOutputTuple;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class NexmarkCount extends splitBolt {
    private static final Logger LOG = LoggerFactory.getLogger(NexmarkCount.class);
    private static final long serialVersionUID = 8089145995668583749L;

    private final Map<Long, Long> counts = new HashMap<>();//what if memory is not enough to hold counts?

    public NexmarkCount () {
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
        return new Fields(Field.WORD);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
    }

    public void execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;

        for (int i = 0; i < bound; i ++) {
            NexmarkTuple tuple = (NexmarkTuple) in.getValue(0, i);
            long key = tuple.auction;
            long v = counts.getOrDefault(key, 0L) + 1;

            counts.put(key, v);
            collector.emit(4, new NexmarkOutputTuple(tuple.auction, v));
        }
    }

    public void profile_execute(TransferTuple in) throws InterruptedException {
        int bound = in.length;
        for (int i = 0; i < bound; i++) {
            NexmarkTuple tuple = (NexmarkTuple) in.getValue(0, i);
            collector.emit_nowait(new NexmarkOutputTuple(tuple.auction, 0));
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("campaign_id", "event_time"));
    }
}

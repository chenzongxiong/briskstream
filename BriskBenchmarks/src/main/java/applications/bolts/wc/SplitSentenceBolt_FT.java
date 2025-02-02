package applications.bolts.wc;

import applications.constants.BaseConstants;
import applications.constants.WordCountConstants.Field;
import applications.util.Configuration;
import applications.util.OsUtils;
import applications.util.datatypes.StreamValues;
import brisk.components.context.TopologyContext;
import brisk.components.operators.api.Checkpointable;
import brisk.components.operators.base.splitBolt;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.execution.runtime.tuple.impl.Marker;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.faulttolerance.impl.ValueState;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

//import static Brisk.utils.Utils.printAddresses;

public class SplitSentenceBolt_FT extends splitBolt implements Checkpointable {
    private static final Logger LOG = LoggerFactory.getLogger(SplitSentenceBolt_FT.class);
    private static final long serialVersionUID = -8704562309673923018L;
//	long end = 0;
//	boolean update = false;
//	int loop = 1;
//	private int curr = 0, precurr = 0;
//	private int dummy = 0;

    public SplitSentenceBolt_FT() {
        super(LOG, new HashMap<>());
        this.output_selectivity.put(BaseConstants.BaseStream.DEFAULT, 10.0);
        state = new ValueState();
    }

    public Integer default_scale(Configuration conf) {
        return conf.getInt("num_socket") * 5;
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.WORD);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException {
        final long bid = in.getBID();
        String value = in.getString(0);
        String splitregex = ",";
        String[] words = value.split(splitregex);//up remote: 14161.599999999988, 13, 14; all local: 13271.8, 0, 15; down remote:11786.49, 0, 14.
        for (String word : words) {
            if (!StringUtils.isBlank(word)) {
                collector.emit(bid, new StreamValues(word));
            }
        }
    }

    public void execute(TransferTuple in) throws InterruptedException {
        //up remote: 14161.599999999988, 13, 14; all local: 13271.8, 0, 15; down remote:11786.49, 0, 14.
        int bound = in.length;
        final long bid = in.getBID();
        for (int i = 0; i < bound; i++) {

            final Marker marker = in.getMarker(i);
            if (marker != null) {
                forward_checkpoint(in.getSourceTask(), bid, marker);
                continue;
            }

            String value = in.getString(0, i);
            String[] split = value.split(",");
            for (String word : split) {
                if (!StringUtils.isBlank(word)) {
                    collector.emit(bid, new StreamValues(word));
                }
            }
        }
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        long pid = OsUtils.getPID(TopologyContext.HPCMonotor);
//		LOG.info("PID  = " + pid);
    }

    @Override
    public void forward_checkpoint(int sourceId, long bid, Marker marker) throws InterruptedException {
        final boolean check = checkpoint_forward(sourceId);//simply forward marker when it is ready.
        if (check) {
            this.collector.broadcast_marker(bid, marker);//bolt needs to broadcast_marker
            //LOG.DEBUG(this.getContext().getThisComponentId() + this.getContext().getThisTaskId() + " broadcast marker with id:" + marker.msgId + "@" + DateTime.now());
        }
    }

    @Override
    public void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker) {

    }

    @Override
    public void ack_checkpoint(Marker marker) {
//		//LOG.DEBUG("Received ack from all consumers.");

        //Do something to clear past state. (optional)

//		//LOG.DEBUG("Broadcast ack to all producers.");
        this.collector.broadcast_ack(marker);//bolt needs to broadcast_ack
    }

    @Override
    public void earlier_ack_checkpoint(Marker marker) {

    }
}

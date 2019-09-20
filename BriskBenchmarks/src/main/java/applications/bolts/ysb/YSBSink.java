package applications.bolts.ysb;

import applications.sink.NullSink;
import brisk.execution.runtime.tuple.TransferTuple;
import brisk.execution.runtime.tuple.impl.Tuple;
import brisk.execution.ExecutionGraph;
import applications.bolts.ysb.model.YSBTuple;
import applications.bolts.ysb.model.YSBOutputTuple;

public class YSBSink extends NullSink {

    @Override
    public void initialize(int threadId, int thisTaskId, ExecutionGraph graph) {
        super.initialize(threadId, thisTaskId, graph);
    }
    @Override
    public void execute(Tuple input) {
        System.out.println("[DBG] YSBSink Receive input tuples");
    }

    @Override
    public void execute(TransferTuple in) throws InterruptedException {
        // System.out.println("[DBG] YSBSink Receive input tuples");
        int bound = in.length;
        for (int i = 0; i < bound; i ++) {
            // YSBTuple tuple = (YSBTuple) in.getValue(0, i);
            // System.out.println("tuple.userId: " + tuple.userId);
            YSBOutputTuple tuple = (YSBOutputTuple) in.getValue(0, i);
            System.out.println("tuple.campaignId: " + tuple.campaignId);
        }
    }
}

package brisk.components.operators.api;

import applications.tools.FastZipfGenerator;
import brisk.execution.runtime.tuple.impl.Marker;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.util.ArrayList;

import static applications.CONTROL.*;
import static applications.Constants.DEFAULT_STREAM_ID;
import static engine.profiler.Metrics.NUM_ACCESSES;

public abstract class TransactionalSpout extends AbstractSpout implements Checkpointable {
    private static final Logger LOG = LoggerFactory.getLogger(TransactionalSpout.class);
    protected transient FastZipfGenerator p_generator;
    protected long previous_bid = -1;
    protected long epoch_size = 0;
    protected double target_Hz;
    protected double checkpoint_interval_sec;
    protected volatile int control = 0;//control how many elements in each epoch.

    protected int element = 0;
    protected ArrayList<String> array;
    protected int counter = 0;

    protected int total_children_tasks = 0;
    protected int tthread;

    protected transient BufferedWriter writer;
    protected int taskId;
    protected int event_counter = 0;
    protected int ccOption;
    protected long bid = 0;//local bid.
    volatile boolean earilier_check = true;

    public int empty = 0;//execute without emit.

    protected TransactionalSpout(Logger log) {
        super(log);
    }

    public double getEmpty() {
        return empty;
    }

    @Override
    public abstract void nextTuple() throws InterruptedException;


    @Override
    public void forward_checkpoint(int sourceId, long bid, Marker marker) throws InterruptedException {
        forward_checkpoint(sourceId, DEFAULT_STREAM_ID, bid, marker);
    }

    @Override
    public void forward_checkpoint_single(int sourceId, long bid, Marker marker) throws InterruptedException {
        forward_checkpoint_single(sourceId, DEFAULT_STREAM_ID, bid, marker);
    }

    @Override
    public void forward_checkpoint_single(int sourceTask, String streamId, long bid, Marker marker) throws InterruptedException {
        if (clock.tick(myiteration) && success) {//emit marker tuple
//			forwardResultAndMark(streamId, values, bid_counter++ % bid_end);
//			final long msgId = bid_counter++;//++ % bid_end;
//            LOG.debug(executor.getOP_full() + " emit marker of: " + myiteration + " @" + DateTime.now());
//            long start = System.nanoTime();
            collector.create_marker_single(boardcast_time, streamId, bid, myiteration);
            boardcast_time = System.nanoTime();
//            LOG.info("Broadcast marker takes:" + (boardcast_time - start));

            myiteration++;
            success = false;

            epoch_size = bid - previous_bid;
            previous_bid = bid;
            earilier_check = true;
        }
    }

    @Override
    public void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker) throws InterruptedException {
        if (clock.tick(myiteration) && success) {//emit marker tuple

            if (!enable_app_combo) {

                LOG.info(executor.getOP_full() + " emit marker of: " + myiteration + " @" + DateTime.now() + " SOURCE_CONTROL: " + bid);
                collector.create_marker_boardcast(boardcast_time, streamId, bid, myiteration);
                boardcast_time = System.nanoTime();
                myiteration++;
                success = false;
                epoch_size = bid - previous_bid;
                previous_bid = bid;
                earilier_check = true;
            } else {


            }
        }
    }

    @Override
    public void ack_checkpoint(Marker marker) {
        //Do something to clear past state. (optional)

        success = true;//I can emit next marker.

        if (enable_debug)
            LOG.trace("task_size: " + epoch_size * NUM_ACCESSES);


        long elapsed_time = System.nanoTime() - boardcast_time;//the time elapsed for the system to handle the previous epoch.
        double actual_system_throughput = epoch_size * 1E9 / elapsed_time;//events/ s
//        if (epoch_size != 0)
//            LOG.info("finished measurement (k events/s):" + actual_system_throughput / 1E3);
        if (enable_admission_control) {
//            target_Hz = actual_system_throughput * checkpoint_interval_sec;//target Hz.
            control = 0;
        }

    }

    @Override
    public void earlier_ack_checkpoint(Marker marker) {
//        if (earilier_check) {
//            control = 0;
//            earilier_check = false;
//        }
    }

    @Override
    public void cleanup() {

    }
}

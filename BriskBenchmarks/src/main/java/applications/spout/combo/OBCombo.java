package applications.spout.combo;

import applications.bolts.ob.OBBolt_lwm;
import applications.bolts.ob.OBBolt_olb;
import applications.bolts.ob.OBBolt_sstore;
import applications.bolts.ob.OBBolt_ts;
import applications.param.ob.AlertEvent;
import applications.param.ob.BuyingEvent;
import applications.param.ob.ToppingEvent;
import applications.util.Configuration;
import applications.util.OsUtils;
import brisk.components.context.TopologyContext;
import brisk.execution.ExecutionGraph;
import brisk.execution.runtime.collector.OutputCollector;
import brisk.faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

import static applications.CONTROL.*;
import static applications.Constants.Event_Path;
import static engine.content.Content.*;

//TODO: Re-name microbenchmark as GS (Grep and Sum).
public class OBCombo extends SPOUTCombo {
    private static final Logger LOG = LoggerFactory.getLogger(OBCombo.class);
    private static final long serialVersionUID = -2394340130331865581L;

    public OBCombo() {
        super(LOG, 0);
        this.scalable = false;
        state = new ValueState();
    }


    @Override
    public void loadEvent(String file_name, Configuration config, TopologyContext context, OutputCollector collector) {
        String event_path = Event_Path
                + OsUtils.OS_wrapper("enable_states_partition=" + String.valueOf(enable_states_partition));

        if (Files.notExists(Paths.get(event_path + OsUtils.OS_wrapper(file_name))))
            throw new UnsupportedOperationException();

        Scanner sc;
        try {
            sc = new Scanner(new File(event_path + OsUtils.OS_wrapper(file_name)));

            int i = 0;
            Object event;
            for (int j = 0; j < taskId * num_events_per_thread; j++) {
                sc.nextLine();//skip un-related.
            }
            while (sc.hasNextLine()) {
                String read = sc.nextLine();
                String[] split = read.split(split_exp);

                if (split[4].endsWith("BuyingEvent")) {//BuyingEvent
                    event = new BuyingEvent(
                            Integer.parseInt(split[0]), //bid
                            split[2], //bid_array
                            Integer.parseInt(split[1]),//pid
                            Integer.parseInt(split[3]),//num_of_partition
                            split[5],//key_array
                            split[6],//price_array
                            split[7]  //qty_array
                    );
                } else if (split[4].endsWith("AlertEvent")) {//AlertEvent
                    event = new AlertEvent(
                            Integer.parseInt(split[0]), //bid
                            split[2], // bid_array
                            Integer.parseInt(split[1]),//pid
                            Integer.parseInt(split[3]),//num_of_partition
                            Integer.parseInt(split[5]), //num_access
                            split[6],//key_array
                            split[7]//price_array
                    );
                } else {
                    event = new ToppingEvent(
                            Integer.parseInt(split[0]), //bid
                            split[2], Integer.parseInt(split[1]), //pid
                            //bid_array
                            Integer.parseInt(split[3]),//num_of_partition
                            Integer.parseInt(split[5]), //num_access
                            split[6],//key_array
                            split[7]  //top_array
                    );
                }
//                db.eventManager.put(event, Integer.parseInt(split[0]));
                myevents[i++] = event;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

        super.initialize(thread_Id, thisTaskId, graph);

        _combo_bid_size = combo_bid_size;

        switch (config.getInt("CCOption", 0)) {
//            case CCOption_LOCK: {//no-order
//                bolt = new SL(0);
//                break;
//            }
            case CCOption_OrderLOCK: {//LOB
                bolt = new OBBolt_olb(0);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_LWM: {//LWM
                bolt = new OBBolt_lwm(0);
                _combo_bid_size = 1;
                break;
            }
            case CCOption_TStream: {//T-Stream
                bolt = new OBBolt_ts(0);
                break;
            }
            case CCOption_SStore: {//SStore
                bolt = new OBBolt_sstore(0);
                _combo_bid_size = 1;
                break;
            }
        }

        //do preparation.
        bolt.prepare(config, context, collector);
        if (enable_shared_state)
            bolt.loadDB(config, context, collector);
    }
}
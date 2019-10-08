package applications.topology;

// import applications.bolts.comm.StringParserBolt;
// import applications.bolts.wc.SplitSentenceBolt;
// import applications.bolts.wc.WordCountBolt;
import applications.bolts.ysb.DeserializeBolt;
import applications.bolts.ysb.EventFilterBolt;
import applications.bolts.ysb.EventProjectionBolt;
import applications.bolts.ysb.YSBMemFileSpout;
import applications.bolts.ysb.YSBSink;
// import applications.constants.WordCountConstants;
// import applications.constants.WordCountConstants.Component;
// import applications.constants.WordCountConstants.Field;
// import applications.constants.YSBConstants;
// import applications.constants.YSBConstants.Component;
// import applications.constants.YSBConstants.Field;

import applications.util.Configuration;
import applications.sink.BaseSink;
import brisk.components.Topology;
import brisk.components.exception.InvalidIDException;
import brisk.components.grouping.FieldsGrouping;
import brisk.components.grouping.ShuffleGrouping;
import brisk.controller.input.scheduler.SequentialScheduler;
import brisk.execution.runtime.tuple.impl.Fields;
import brisk.topology.BasicTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import brisk.components.operators.api.AbstractSpout;

// import static applications.constants.YSBConstants.PREFIX;

public class YSB extends BasicTopology {
    private static final String PREFIX = "ysb";

    private static final Logger LOG = LoggerFactory.getLogger(YSB.class);

    public YSB(String topologyName, Configuration config) {
        super("[YSB]", config);
//        initilize_parser();
    }

    public static String getPrefix() {
        return PREFIX;
    }

    public void initialize() {
        System.out.println("[DBG] Initialize YSB topology");
        loadSpout();
        loadSink();
    }

    @Override
    protected AbstractSpout loadSpout() {
        if (spout == null) {
            spout = new YSBMemFileSpout();
        }
        return (AbstractSpout) spout;
    }

    @Override
    protected BaseSink loadSink() {
        if (sink == null) {
            sink = new YSBSink();
        }
        return (BaseSink) sink;
    }

    // @Override
    // protected void initialize_parser() {
    //     // if (parser == null) {
    //     //     parser = (Parser)(new DeserializeBolt());
    //     // }
    // }

    @Override
    public Topology buildTopology() {
        int parallelism = 8;
        try {
            builder.setSpout("ysbSpout", spout, 4);
            builder.setBolt("ysbDeserializeBolt", new DeserializeBolt(), parallelism,
                            new ShuffleGrouping("ysbSpout"));

            builder.setBolt("ysbEventBolt", new EventFilterBolt(), parallelism,
                            // new FieldsGrouping("ysbDeserializeBolt", new Fields("campaign_id")));
                            new ShuffleGrouping("ysbDeserializeBolt"));

            builder.setBolt("ysbProjectionBolt", new EventProjectionBolt(), parallelism,
                            // new FieldsGrouping("ysbDeserializeBolt", new Fields("campaign_id")));
                            new ShuffleGrouping("ysbEventBolt"));

            // builder.setSink("ysbSink", sink, 1,
            //                 new ShuffleGrouping("ysbProjectionBolt"));

            builder.setSink("ysbSink", sink, parallelism,
                            new ShuffleGrouping("ysbProjectionBolt"));

        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology();
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }

    @Override
    public String getConfigPrefix() {
        return PREFIX;
    }
}

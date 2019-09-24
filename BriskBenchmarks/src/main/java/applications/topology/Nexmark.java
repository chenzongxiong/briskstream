package applications.topology;

import applications.bolts.nexmark.NexmarkMemFileSpout;
import applications.bolts.nexmark.NexmarkSink;
import applications.bolts.nexmark.NexmarkParser;

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


public class Nexmark extends BasicTopology {
    private static final String PREFIX = "ysb";

    private static final Logger LOG = LoggerFactory.getLogger(Nexmark.class);

    public Nexmark(String topologyName, Configuration config) {
        super("[Nexmark]", config);
//        initilize_parser();
    }

    public static String getPrefix() {
        return PREFIX;
    }

    public void initialize() {
        System.out.println("[DBG] Initialize Nexmark topology");
        loadSpout();
        loadSink();
    }

    @Override
    protected AbstractSpout loadSpout() {
        if (spout == null) {
            spout = new NexmarkMemFileSpout();
        }
        return (AbstractSpout) spout;
    }

    @Override
    protected BaseSink loadSink() {
        if (sink == null) {
            sink = new NexmarkSink();
        }
        return (BaseSink) sink;
    }

    @Override
    public Topology buildTopology() {
        int parallelism = 1;
        try {
            builder.setSpout("nexmarkSpout", spout, 1);
            builder.setBolt("nexmarkParser", new NexmarkParser(), parallelism,
                            new ShuffleGrouping("nexmarkSpout"));

            // builder.setBolt("ysbEventBolt", new EventFilterBolt(), parallelism,
            //                 // new FieldsGrouping("nexmarkParser", new Fields("campaign_id")));
            //                 new ShuffleGrouping("nexmarkParser"));

            // builder.setBolt("ysbProjectionBolt", new EventProjectionBolt(), parallelism,
            //                 // new FieldsGrouping("nexmarkParser", new Fields("campaign_id")));
            //                 new ShuffleGrouping("ysbEventBolt"));

            // // builder.setSink("ysbSink", sink, 1,
            // //                 new ShuffleGrouping("ysbProjectionBolt"));

            builder.setSink("nexmarkSink", sink, parallelism,
                            new ShuffleGrouping("nexmarkParser"));

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

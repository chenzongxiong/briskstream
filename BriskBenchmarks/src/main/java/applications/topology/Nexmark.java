package applications.topology;

import applications.bolts.nexmark.NexmarkMemFileSpout;
import applications.bolts.nexmark.NexmarkSink;
import applications.bolts.nexmark.NexmarkParser;
import applications.bolts.nexmark.NexmarkFilter;
import applications.bolts.nexmark.NexmarkMapper;
import applications.bolts.nexmark.NexmarkCount;
import applications.bolts.nexmark.NexmarkMaxAuctionCount;
import applications.bolts.nexmark.NexmarkMaxPrice;

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
    private static final String PREFIX = "nexmark";

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
        int parallelism = 24;
        // return this.buildTopologyQ1(parallelism);
        // return this.buildTopologyQ2(parallelism);
        return this.buildTopologyQ3(parallelism);
        // return this.buildTopologyQ4(parallelism);
    }


    private Topology buildTopologyQ1(int parallelism) {
        try {
            // builder.setSpout("nexmarkSpout", spout, parallelism);
            builder.setSpout("nexmarkSpout", spout, 1);
            builder.setBolt("nexmarkParser", new NexmarkParser(), parallelism,
                            new ShuffleGrouping("nexmarkSpout"));

            builder.setBolt("nexmarkMapper", new NexmarkMapper(), parallelism,
                            new ShuffleGrouping("nexmarkParser"));
            builder.setSink("nexmarkSink", sink, parallelism,
                            new ShuffleGrouping("nexmarkMapper"));

            // builder.setSink("nexmarkSink", sink, parallelism,
            //                 new ShuffleGrouping("nexmarkSpout"));

        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology();
    }

    private Topology buildTopologyQ2(int parallelism) {
        try {
            builder.setSpout("nexmarkSpout", spout, 2);
            builder.setBolt("nexmarkParser", new NexmarkParser(), parallelism,
                            new ShuffleGrouping("nexmarkSpout"));

            builder.setBolt("nexmarkFilter", new NexmarkFilter(), parallelism,
                            new ShuffleGrouping("nexmarkParser"));

            builder.setSink("nexmarkSink", sink, parallelism,
                            new ShuffleGrouping("nexmarkFilter"));

        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology();
    }

    private Topology buildTopologyQ3(int parallelism) {
        try {
            builder.setSpout("nexmarkSpout", spout, 4);
            builder.setBolt("nexmarkParser", new NexmarkParser(), parallelism,
                            new ShuffleGrouping("nexmarkSpout"));

            builder.setBolt("nexmarkCount", new NexmarkCount(), parallelism,
                            new ShuffleGrouping("nexmarkParser"));

            builder.setBolt("nexmarkMaxAuctionCount", new NexmarkMaxAuctionCount(), parallelism,
                            new ShuffleGrouping("nexmarkCount"));

            // builder.setSink("nexmarkSink", sink, parallelism,
            //                 new ShuffleGrouping("nexmarkMaxAuctionCount"));

            builder.setSink("nexmarkSink", sink, parallelism,
                            new ShuffleGrouping("nexmarkMaxAuctionCount"));

        } catch (InvalidIDException e) {
            e.printStackTrace();
        }
        builder.setGlobalScheduler(new SequentialScheduler());
        return builder.createTopology();
    }

    private Topology buildTopologyQ4(int parallelism) {
        try {
            builder.setSpout("nexmarkSpout", spout, 1);
            builder.setBolt("nexmarkParser", new NexmarkParser(), parallelism,
                            new ShuffleGrouping("nexmarkSpout"));

            builder.setBolt("nexmarkMaxPrice", new NexmarkMaxPrice(), parallelism,
                            new ShuffleGrouping("nexmarkParser"));

            builder.setSink("nexmarkSink", sink, parallelism,
                            new ShuffleGrouping("nexmarkMaxPrice"));

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

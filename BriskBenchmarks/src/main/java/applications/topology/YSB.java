package applications.topology;

import applications.bolts.comm.StringParserBolt;
// import applications.bolts.wc.SplitSentenceBolt;
// import applications.bolts.wc.WordCountBolt;
// import applications.bolts.ysb.DeserialzeBolt;

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
        super.initialize();
        sink = loadSink();
    }

    @Override
    protected AbstractSpout loadSpout() {
        return null;
    }

    @Override
    protected BaseSink loadSink() {
        return null;
    }

    @Override
    public Topology buildTopology() {

        // try {
        //     // spout.setFields(new Fields(Field.TEXT));
        //     // builder.setSpout(Component.SPOUT, spout, 1);

        //     // builder.setSpout("ads", kafkaSpout, kafkaPartitions);
        // } catch (InvalidIDException e) {
        //     e.printStackTrace();
        // }
        // builder.setGlobalScheduler(new SequentialScheduler());
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

package applications;

import applications.topology.benchmarks.*;
import applications.util.Configuration;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class HeronRunner extends abstractRunner {
    private static final Logger LOG = LoggerFactory.getLogger(HeronRunner.class);
    public static LocalCluster cluster;
    private final AppDriver driver;
    public Config config = new Config();

    public HeronRunner() {
        driver = new AppDriver();
//        driver.addApp("StreamingAnalysis", StreamingAnalysis.class);//Extra
        driver.addApp("WordCount", WordCount.class);
        driver.addApp("FraudDetection", FraudDetection.class);
        driver.addApp("SpikeDetection", SpikeDetection.class);
//        driver.addApp("TrafficMonitoring", TrafficMonitoring.class);
        driver.addApp("LogProcessing", LogProcessing.class);
        driver.addApp("VoIPSTREAM", VoIPSTREAM.class);
        driver.addApp("LinearRoad", LinearRoad.class);//
    }

    public static void main(String[] args) {

        HeronRunner runner = new HeronRunner();
        JCommander cmd = new JCommander(runner);
//        Kryo kryo = new Kryo();
//        kryo.register(java.utils.Date.class);
//
        try {
            cmd.parse(args);
        } catch (ParameterException ex) {
            System.err.println("Argument error: " + ex.getMessage());
            cmd.usage();
            System.exit(1);
        }
        try {
            runner.run();
        } catch (AlreadyAliveException | InvalidTopologyException ex) {
            LOG.error("Error in running Brisk.topology remotely", ex);
        } catch (InterruptedException ex) {
            LOG.error("Error in running Brisk.topology locally", ex);
        }

    }

    /**
     * Run the Brisk.topology locally
     *
     * @param topology         The Brisk.topology to be executed
     * @param topologyName     The name of the Brisk.topology
     * @param conf             The configurations for the Brisk.execution
     * @param runtimeInSeconds For how much time the Brisk.topology will run
     * @throws InterruptedException
     */
    public static void runTopologyLocally(StormTopology topology, String topologyName, Config conf,
                                          int runtimeInSeconds) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        Configuration Conf = Configuration.fromMap(conf);
        conf.setMaxSpoutPending(Conf.getInt("max_pending", 5000));
        //conf.put("compatibility.zookeeper.session.timeout", 300000);
        conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, 300000);
        LOG.info("Starting Storm on local mode to run for {} seconds", runtimeInSeconds);
        cluster = new LocalCluster();
        LOG.info("Topology {} submitted", topologyName);
        cluster.submitTopology(topologyName, conf, topology);
        Thread.sleep((long) runtimeInSeconds * 1000);
        cluster.shutdown();
    }

    /**
     * Run the Brisk.topology remotely
     *
     * @param topology     The Brisk.topology to be executed
     * @param topologyName The name of the Brisk.topology
     * @param conf         The configurations for the Brisk.execution
     * @throws AlreadyAliveException
     * @throws InvalidTopologyException
     */
    public static void runTopologyRemotely(StormTopology topology, String topologyName, Config conf) {

        // conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);

        Configuration Conf = Configuration.fromMap(conf);
        //conf.setMaxSpoutPending(Conf.getInt("max_pending", 5000));
        // This will simply log all Metrics received into
        // $STORM_HOME/logs/metrics.log on one or more worker nodes.
        // conf.registerMetricsConsumer(LoggingMetricsConsumer.class, 2);

        //LOG.info("max pending;" + conf.get("Brisk.topology.max.spout.pending"));
        //LOG.info("metrics.output:" + Conf.getString("metrics.output"));
        //LOG.info("NumWorkers:" + Conf.getInt("num_workers"));
        LOG.info("Run with configuration:" + conf.values());

        try {
            StormSubmitter.submitTopology(topologyName, conf, topology);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public void run() throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        // Loads the configuration file set by the user or the default
        // configuration
        try {
            // load default configuration
            if (configStr == null) {

                String cfg = String.format(CFG_PATH, application);
                Properties p = loadProperties(cfg, (configStr == null));


                config.put(BaseConstants.BaseConf.SPOUT_THREADS, sthread);
                config.put(BaseConstants.BaseConf.SINK_THREADS, sithread);

                config.putAll(Configuration.fromProperties(p));
                if (mode.equalsIgnoreCase(RUN_REMOTE)) {
                    final String spout_class = String.valueOf(config.get("mb.spout.class"));
                    if (spout_class.equals("applications.spout.LocalStateSpout")) {
                        LOG.info("Please use kafkaSpout in cluster mode!!!");
                        System.exit(-1);
                    }
                }
                config.setDebug(false);
                config.put(Config.TOPOLOGY_WORKER_CHILDOPTS, CHILDOPTS);
                configuration(config);
            } else {
                config.putAll(Configuration.fromStr(configStr));
                LOG.info("Loaded configuration from command line argument");
            }
        } catch (IOException ex) {
            LOG.error("Unable to load configuration file", ex);
            throw new RuntimeException("Unable to load configuration file", ex);
        }

        // Get the descriptor for the given application
        AppDriver.AppDescriptor app = driver.getApp(application);
        if (app == null) {
            throw new RuntimeException("The given application name " + application + " is invalid");
        }

        // In case no Brisk.topology names is given, create one
        if (topologyName == null) {
            topologyName = application;
        }
        config.put(BaseConstants.BaseConf.SPOUT_THREADS, sthread);
        config.put(BaseConstants.BaseConf.SINK_THREADS, sithread);
        config.put(BaseConstants.BaseConf.PARSER_THREADS, pthread);
        //set total parallelism, equally parallelism
        int max_hz = 0;
        switch (application) {
            case "StreamingAnalysis": {
                int threads = (int) Math.ceil(tthread / 5.0);
                config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);
                config.put(streamingAnalysisConstants.Conf.EXECUTOR_THREADS1, threads);
                config.put(streamingAnalysisConstants.Conf.EXECUTOR_THREADS2, threads);
                config.put(streamingAnalysisConstants.Conf.EXECUTOR_THREADS3, threads);
                config.put(streamingAnalysisConstants.Conf.EXECUTOR_THREADS4, threads);
                break;
            }
            case "WordCount": {
                //config.put(BaseConstants.BaseConf.SPOUT_THREADS, 2);//special treatment to WC>
                int threads = (int) Math.ceil(tthread / 3.0);
                LOG.info("Average threads:" + threads);
                config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);//insignificant
                config.put(WordCountConstants.Conf.SPLITTER_THREADS, threads);//2
                config.put(WordCountConstants.Conf.COUNTER_THREADS, threads);
                max_hz = WordCountConstants.max_hz;
                break;
            }
            case "FraudDetection": {
                config.put(BaseConstants.BaseConf.SPOUT_THREADS, 16);//special treatment to FD>
                int threads = (int) Math.ceil(tthread / 2.0);
                config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);//insignificant
                config.put(FraudDetectionConstants.Conf.PREDICTOR_THREADS, threads);
                max_hz = FraudDetectionConstants.max_hz;
                break;
            }
            case "SpikeDetection": {
//                config.put(BaseConstants.BaseConf.SPOUT_THREADS, 1);//special treatment to SD>
                int threads = (int) Math.ceil(tthread / 3.0);
                LOG.info("Average threads:" + threads);
                config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);
                config.put(SpikeDetectionConstants.Conf.MOVING_AVERAGE_THREADS, threads);//insignificant
                config.put(SpikeDetectionConstants.Conf.SPIKE_DETECTOR_THREADS, threads);//insignificant
                max_hz = SpikeDetectionConstants.max_hz;
                break;
            }
//                    case "TrafficMonitoring": {
//                        int threads = tthread / 1;
////                        config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);
//                        config.put(TrafficMonitoringConstants.Conf.MAP_MATCHER_THREADS, threads);//*5
////                        config.put(TrafficMonitoringConstants.Conf.SPEED_CALCULATOR_THREADS, threads);
//                        max_hz = TrafficMonitoringConstants.max_hz;
//                        break;
//                    }
            case "LogProcessing": {
                config.put(BaseConstants.BaseConf.SPOUT_THREADS, 1);//special treatment to LG>
                int threads = (int) Math.ceil(tthread / 5.0);
                LOG.info("Average threads:" + threads);
                config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);
                config.put(LogProcessingConstants.Conf.GEO_FINDER_THREADS, threads);//2
                config.put(LogProcessingConstants.Conf.GEO_STATS_THREADS, threads);//insignificant
                config.put(LogProcessingConstants.Conf.STATUS_COUNTER_THREADS, threads);//insignificant
                config.put(LogProcessingConstants.Conf.VOLUME_COUNTER_THREADS, threads);//insignificant
                break;
            }
            case "VoIPSTREAM": {
                int threads = (int) Math.ceil(tthread / 11.0);
                LOG.info("Average threads:" + threads);
                config.put(BaseConstants.BaseConf.PARSER_THREADS, threads);
                config.put(VoIPSTREAMConstants.Conf.VAR_DETECT_THREADS, threads);
                config.put(VoIPSTREAMConstants.Conf.RCR_THREADS, threads);//2
                config.put(VoIPSTREAMConstants.Conf.ECR_THREADS, threads);//2
                config.put(VoIPSTREAMConstants.Conf.ENCR_THREADS, threads);//insignificant
                config.put(VoIPSTREAMConstants.Conf.CT24_THREADS, threads);//insignificant
                config.put(VoIPSTREAMConstants.Conf.ECR24_THREADS, threads);
                //   config.put(VoIPSTREAMConstants.Conf.GLOBAL_ACD, threads); 1
                config.put(VoIPSTREAMConstants.Conf.FOFIR_THREADS, threads);//2
                config.put(VoIPSTREAMConstants.Conf.URL_THREADS, threads);
                config.put(VoIPSTREAMConstants.Conf.ACD_THREADS, threads);
                config.put(VoIPSTREAMConstants.Conf.SCORER_THREADS, threads);
                break;
            }
            case "LinearRoad": {
//                config.put(BaseConstants.BaseConf.SPOUT_THREADS, 2);//special treatment to LR>
                int threads = (int) Math.ceil(tthread / 7.0);
                LOG.info("Average threads:" + threads);
                config.put(LinearRoadConstants.Conf.DispatcherBoltThreads, threads);
                config.put(LinearRoadConstants.Conf.accidentBoltThreads, threads);//insignificant
                config.put(LinearRoadConstants.Conf.CountThreads, threads);//insignificant
                config.put(LinearRoadConstants.Conf.dailyExpBoltThreads, threads);//insignificant
                config.put(LinearRoadConstants.Conf.AccidentNotificationBoltThreads, threads);//insignificant
                config.put(LinearRoadConstants.Conf.tollBoltThreads, threads);//insignificant
                config.put(LinearRoadConstants.Conf.AccountBalanceBoltThreads, threads);
                break;
            }
        }

        config.registerSerialization(java.util.Date.class);
//        config.registerSerialization(org.apache.compatibility.tuple.TupleImpl.class);
        // Get the Brisk.topology and execute on Storm
        StormTopology stormTopology = app.getTopology(topologyName, config);
        config.put("Brisk.topology.acker.executors", 0);
        switch (mode) {
            case RUN_LOCAL:
                runTopologyLocally(stormTopology, topologyName, config, runtimeInSeconds * 2);
                break;
            case RUN_REMOTE:
                runTopologyRemotely(stormTopology, topologyName, config);
                break;
            default:
                throw new RuntimeException("Valid running modes are 'local' and 'remote'");
        }
    }
}

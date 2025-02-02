package applications.bolts.fd;

import applications.bolts.AbstractBolt;
import applications.constants.FraudDetectionConstants;
import applications.model.predictor.MarkovModelPredictor;
import applications.model.predictor.ModelBasedPredictor;
import applications.model.predictor.Prediction;
import org.apache.commons.lang3.StringUtils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static applications.Constants.Marker_STREAM_ID;
import static applications.constants.BaseConstants.BaseField.MSG_ID;
import static applications.constants.BaseConstants.BaseField.SYSTEMTIMESTAMP;

/**
 * @author maycon
 */
public class FraudPredictorBolt extends AbstractBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FraudPredictorBolt.class);
    double sel = 0;
    double nsel = 0;
    int cnt = 0;
    int loop = 1;
    private ModelBasedPredictor predictor;

    @Override
    public void initialize() {
        String strategy = config.getString(FraudDetectionConstants.Conf.PREDICTOR_MODEL);

        if (strategy.equals("mm")) {
            predictor = new MarkovModelPredictor(config);
        }
    }

    @Override
    public void execute(Tuple input) {

        String entityID = input.getString(0);
        String record = input.getString(1);
        Prediction p = predictor.execute("Z2E821O6VB".toCharArray(), "23UYUALXNS6M,LHL".toCharArray());//use fixed input to simulate the running.

        // send outliers
        if (p.isOutlier()) {
            //  StableValues.create(entityID, p.getScore(), StringUtils.join(p.getStates(), ","));
//            sel++;
            collector.emit(new Values(entityID, p.getScore(), StringUtils.join(p.getStates(), ",")));
        }
    }

    public void display() {
        //LOG.info("cnt:" + cnt + "\tcnt1:" + sel + "(" + (sel / cnt) + ")");
    }

    @Override
    public Fields getDefaultFields() {

        this.fields.put(Marker_STREAM_ID, new Fields(
                FraudDetectionConstants.Field.ENTITY_ID,
                FraudDetectionConstants.Field.SCORE,
                FraudDetectionConstants.Field.STATES,
                MSG_ID, SYSTEMTIMESTAMP));

        return new Fields(
                FraudDetectionConstants.Field.ENTITY_ID,
                FraudDetectionConstants.Field.SCORE,
                FraudDetectionConstants.Field.STATES);
    }
}
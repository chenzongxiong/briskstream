package applications.bolts.lg;

import applications.bolts.AbstractBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static applications.constants.ClickAnalyticsConstants.Field;

/**
 * User: domenicosolazzo
 */
public class GeoStatsBolt extends AbstractBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GeoStatsBolt.class);

    private Map<String, CountryStats> stats;

    @Override
    public void initialize() {
        stats = new HashMap<>();
        LOG.info(Thread.currentThread().getName());
    }

    @Override
    public void execute(Tuple input) {
//        if (stat != null) stat.start_measure();
        String country = (String) input.getValue(0);
        String city = (String) input.getValue(1);

        if (!stats.containsKey(country)) {
            stats.put(country, new CountryStats(country));
        }

        stats.get(country).cityFound(city);
        collector.emit(new Values(country, stats.get(country).getCountryTotal(), city, stats.get(country).getCityTotal(city)));
//        if (stat != null) stat.end_measure();
    }

    @Override
    public Fields getDefaultFields() {
        return new Fields(Field.COUNTRY, Field.COUNTRY_TOTAL, Field.CITY, Field.CITY_TOTAL);
    }

    private class CountryStats {
        private static final int COUNT_INDEX = 0;
        private static final int PERCENTAGE_INDEX = 1;
        private final String countryName;
        private final Map<String, List<Integer>> cityStats = new HashMap<>();
        private int countryTotal = 0;

        public CountryStats(String countryName) {
            this.countryName = countryName;
        }

        public void cityFound(String cityName) {
            countryTotal++;

            if (cityStats.containsKey(cityName)) {
                cityStats.get(cityName).set(COUNT_INDEX, cityStats.get(cityName).get(COUNT_INDEX) + 1);
            } else {
                List<Integer> list = new LinkedList<>();
                list.add(1);
                list.add(0);
                cityStats.put(cityName, list);
            }

            double percent = (double) cityStats.get(cityName).get(COUNT_INDEX) / (double) countryTotal;
            cityStats.get(cityName).set(PERCENTAGE_INDEX, (int) percent);
        }

        public int getCountryTotal() {
            return countryTotal;
        }

        public int getCityTotal(String cityName) {
            return cityStats.get(cityName).get(COUNT_INDEX);
        }

        @Override
        public String toString() {
            return "Total Count for " + countryName + " is " + Integer.toString(countryTotal) + "\n"
                    + "Cities: " + cityStats.toString();
        }
    }
}

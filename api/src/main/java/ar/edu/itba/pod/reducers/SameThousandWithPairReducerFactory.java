package ar.edu.itba.pod.reducers;

import ar.edu.itba.pod.results.AirportPairResult;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class SameThousandWithPairReducerFactory implements ReducerFactory<Long, List<String>, List<AirportPairResult>> {
    private static final long serialVersionUID = 1L;

    @Override
    public Reducer<List<String>, List<AirportPairResult>> newReducer(Long key){
        return new SameThousandWithPairReducer(key);
    }

    private class SameThousandWithPairReducer extends Reducer<List<String>, List<AirportPairResult>> {
        private volatile Long thousand;
        private volatile List<String> temporalList;
        private volatile List<AirportPairResult> airportPairsList;

        public SameThousandWithPairReducer(Long thousand) {
            this.thousand = thousand;
        }

        @Override
        public void beginReduce() {
            temporalList = new ArrayList<>();
        }

        @Override
        public void reduce(List<String> values) {
            values.forEach(value -> temporalList.add(value));
        }

        @Override
        public List<AirportPairResult> finalizeReduce() {
            airportPairsList = new ArrayList<>();
            for (int i = 0; i < temporalList.size() - 1; i++) {
                for (int j = i + 1; j < temporalList.size(); j++) {
                    airportPairsList.add(new AirportPairResult(thousand, temporalList.get(i), temporalList.get(j)));
                }
            }

            return airportPairsList;
        }
    }
}
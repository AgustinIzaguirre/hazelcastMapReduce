package ar.edu.itba.pod.collators;

import ar.edu.itba.pod.results.AirportPairResult;
import com.hazelcast.mapreduce.Collator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JoinPairListCollator implements Collator<Map.Entry<Long, List<AirportPairResult>>, List<AirportPairResult>> {


    @Override
    public List<AirportPairResult> collate(Iterable<Map.Entry<Long, List<AirportPairResult>>> values) {
            List<AirportPairResult> airportPairList = new ArrayList<>();
            values.forEach(element -> airportPairList.addAll(element.getValue()));
            airportPairList.sort((pair1, pair2) -> {

                if (pair1.getMovements() != pair2.getMovements()) {
                    return (int) (pair2.getMovements() - pair1.getMovements());
                }
                else {

                    if (pair1.getFirst().compareTo(pair2.getFirst()) == 0) {
                        return pair1.getSecond().compareTo(pair2.getSecond());
                    }

                    return pair1.getFirst().compareTo(pair2.getFirst());
                }
            });

            return airportPairList;
    }
}

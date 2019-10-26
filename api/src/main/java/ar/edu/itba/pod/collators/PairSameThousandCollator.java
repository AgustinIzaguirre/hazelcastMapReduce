package ar.edu.itba.pod.collators;

import ar.edu.itba.pod.results.AirportPairResult;
import com.hazelcast.mapreduce.Collator;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PairSameThousandCollator implements Collator<Map.Entry<Long, List<String>>, List<AirportPairResult>> {


    @Override
    public List<AirportPairResult> collate(Iterable<Map.Entry<Long, List<String>>> values) {
        List<AirportPairResult> airportPairList = new ArrayList<>();
        values.forEach(element -> {
            List<AirportPairResult> pairResults = getAllPair(element.getKey(), element.getValue());
            if (pairResults.size() > 0) {
                airportPairList.addAll(pairResults); //TODO improve with java 8
            }
        });
        airportPairList.sort((pair1, pair2) -> {
            if (pair1.getMovements() != pair2.getMovements()) {
                return (int) (pair2.getMovements() - pair1.getMovements());
            } else {
                if (pair1.getFirst().compareTo(pair2.getFirst()) == 0) {
                    return pair1.getSecond().compareTo(pair2.getSecond());
                }
                return pair1.getFirst().compareTo(pair2.getFirst());
            }
        });

        return airportPairList;
    }

    private List<AirportPairResult> getAllPair(Long thousands, List<String> airports) {
        List<AirportPairResult> pairResultList = new LinkedList<>();

        if (airports.size() < 2) {
            return pairResultList;
        }

        for (int i = 0; i < airports.size() - 1; i++) {
            for (int j = i + 1; j < airports.size(); j++) {
                pairResultList.add(new AirportPairResult(thousands, airports.get(i), airports.get(j)));
            }
        }

        return pairResultList;
    }
}

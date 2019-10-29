package ar.edu.itba.pod.collators;

import ar.edu.itba.pod.results.AirportPairResult;
import ar.edu.itba.pod.results.AirportsMovementResult;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

public class PairAirportCollator implements Collator<Map.Entry<String, Long>, List<AirportPairResult>> {

    @Override
    public List<AirportPairResult> collate(Iterable<Map.Entry<String, Long>> values ) {
        Map<Long, List<String>> airportsOfThousands = getAirportOfThousands(values);
        List<AirportPairResult> airportPairList = new ArrayList<>();
        airportsOfThousands.forEach((key,value) -> {
            List<AirportPairResult> pairResults = getAllPair(key, value);

            if(pairResults.size() > 0) {
                airportPairList.addAll(pairResults);
            }
        });

        airportPairList.sort((pair1, pair2) -> {

            if(pair1.getMovements() != pair2.getMovements()) {
               return (int)(pair2.getMovements() - pair1.getMovements());
            }
            else {

                if(pair1.getFirst().compareTo(pair2.getFirst()) == 0) {
                    return pair1.getSecond().compareTo(pair2.getSecond());
                }

                return pair1.getFirst().compareTo(pair2.getFirst());
            }
        });

        return airportPairList;
    }



    private Map<Long,List<String>> getAirportOfThousands(Iterable<Map.Entry<String, Long>> values) {
        Map<Long, List<String>> airportsOfThousands = new HashMap<>();
        values.forEach(element -> {

            if(element.getValue() >= 1000) {
                long thousands = (element.getValue() / 1000) * 1000;

                airportsOfThousands.computeIfAbsent(thousands, k -> new LinkedList<>());

                List<String> airports = airportsOfThousands.get(thousands);
                airports.add(element.getKey());
            }
        });

        return airportsOfThousands;
    }

    private List<AirportPairResult> getAllPair(Long thousands, List<String> airports) {
        List<AirportPairResult> pairResultList = new LinkedList<>();

        if(airports.size() < 2) {
            return pairResultList;
        }

        for(int i = 0; i < airports.size() - 1; i++) {
            for(int j = i + 1; j < airports.size(); j++) {
                pairResultList.add(new AirportPairResult(thousands, airports.get(i), airports.get(j)));
            }
        }

        return pairResultList;
    }
}

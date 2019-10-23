package ar.edu.itba.pod.collators;

import ar.edu.itba.pod.results.AirportsMovementResult;
import com.hazelcast.mapreduce.Collator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CabotageFlightsCollator implements Collator<Map.Entry<String, Long>, List<AirportsMovementResult>> {
    private long quantity;

    public CabotageFlightsCollator(long quantity) {
        this.quantity = quantity;
    }

    @Override
    public List<AirportsMovementResult> collate(Iterable<Map.Entry<String, Long>> values ) {
        List<AirportsMovementResult> resultList = new ArrayList<>();
        AtomicLong totalOthers = new AtomicLong();
        values.forEach((entry)-> {
            if(entry.getKey().equals("Otros")) {
                totalOthers.addAndGet(entry.getValue());
            }
            else {
                resultList.add(new AirportsMovementResult(entry.getKey(), entry.getValue()));
            }
        });
        resultList.sort((r1,r2)-> {
            if(r1.getMovements() != r2.getMovements()) {
                return r2.getMovements().compareTo(r1.getMovements());
            }
            else {
                return r1.getKey().compareTo(r2.getKey());
            }
        });

        for(long i = quantity; i < resultList.size(); i++) {
            totalOthers.addAndGet(resultList.get((int)i).getMovements());
        }

        List<AirportsMovementResult> completeList =  resultList.subList(0, (int)quantity);
        completeList.add(new AirportsMovementResult("Otros", totalOthers.get()));
        return completeList;
    }
}

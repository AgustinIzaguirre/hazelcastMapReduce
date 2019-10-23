package ar.edu.itba.pod.collators;

import ar.edu.itba.pod.results.AirportsMovementResult;
import com.hazelcast.mapreduce.Collator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DestinationAirportCollator implements Collator<Map.Entry<String, Long>, List<AirportsMovementResult>> {

    private long quantity;

    public DestinationAirportCollator(long quantity) {
        this.quantity = quantity;
    }

    @Override
    public List<AirportsMovementResult> collate(Iterable<Map.Entry<String, Long>> values ) {
        List<AirportsMovementResult> resultList = new ArrayList<>();
        values.forEach((entry)-> resultList.add(new AirportsMovementResult(entry.getKey(), entry.getValue())));
        resultList.sort((r1,r2)-> {
            if(r1.getMovements() != r2.getMovements()) {
                return r2.getMovements().compareTo(r1.getMovements());
            }
            else {
                return r1.getKey().compareTo(r2.getKey());
            }
        });

        return resultList.subList(0, (int)quantity);
    }
}

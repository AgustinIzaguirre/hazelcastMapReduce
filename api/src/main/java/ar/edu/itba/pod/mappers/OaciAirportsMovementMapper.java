package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.Movement;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.StringTokenizer;

public class OaciAirportsMovementMapper implements Mapper<Long, Movement, String, Long>  {
    @Override
    public void map(Long id, Movement movement, Context<String, Long> context) {
        if(movement.getMovementType().equals("Despegue")) {
            context.emit(movement.getOrigin(), 1L);
        }
        else {
            context.emit(movement.getDestination(), 1L);
        }
    }
}

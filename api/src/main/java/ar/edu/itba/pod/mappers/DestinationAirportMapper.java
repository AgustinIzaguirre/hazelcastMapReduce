package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.Movement;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class DestinationAirportMapper implements Mapper<Long, Movement, String, Long> {
    private String origin;

    public DestinationAirportMapper(String origin) {
        this.origin = origin;
    }

    @Override
    public void map(Long id, Movement movement, Context<String, Long> context) {
        if(movement.getOrigin().equals(origin) && movement.getMovementType().equals("Despegue")) {//TODO maybe on file loader add everything lowercase and compare always lowercase
            context.emit(movement.getDestination(), 1L);
        }
    }
}

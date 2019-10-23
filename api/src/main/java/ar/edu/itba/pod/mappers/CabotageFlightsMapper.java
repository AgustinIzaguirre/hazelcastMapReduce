package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.Movement;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class CabotageFlightsMapper implements Mapper<Long, Movement, String, Long> {

    @Override
    public void map(Long id, Movement movement, Context<String, Long> context) {
        if(movement.getClasification().equals("Cabotaje")) {
            if(movement.getAirline() != null && !movement.getAirline().equals("N/A")) {
                context.emit(movement.getAirline(), 1L);
            }
            else {
                context.emit("Otros", 1L);
            }
        }
    }
}

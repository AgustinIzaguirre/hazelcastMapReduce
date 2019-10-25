package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.Airport;
import ar.edu.itba.pod.models.Movement;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Map;

public class DestinationAirportMapperWithInstanceAware implements HazelcastInstanceAware, Mapper<Long, Movement, String, Long> {
    private transient HazelcastInstance hazelcastInstance;
    private String origin;

    public DestinationAirportMapperWithInstanceAware(String origin) {
        this.origin = origin;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public void map(Long id, Movement movement, Context<String, Long> context) {
         Map<String, Airport > airportsMap = hazelcastInstance.getMap("g12-aeropuertos");

        if(movement.getOrigin().equals(origin) && movement.getMovementType().equals("Despegue")) {//TODO maybe on file loader add everything lowercase and compare always lowercase
            if(airportsMap.get(movement.getDestination()) != null) { //TODO check if it was destination or origin
                context.emit(movement.getDestination(), 1L);
            }
        }
    }
}

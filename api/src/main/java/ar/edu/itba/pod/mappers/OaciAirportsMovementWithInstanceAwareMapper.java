package ar.edu.itba.pod.mappers;

import ar.edu.itba.pod.models.Airport;
import ar.edu.itba.pod.models.Movement;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Map;

public class OaciAirportsMovementWithInstanceAwareMapper implements HazelcastInstanceAware, Mapper<Long, Movement, String, Long> {
    private static final long serialVersionUID = 1L;
    private transient HazelcastInstance hazelcastInstance;


    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public void map(Long id, Movement movement, Context<String, Long> context) {
        Map<String, Airport > airportsMap = hazelcastInstance.getMap("g12-aeropuertos");

        if(movement.getMovementType().equals("Despegue")) {

            if(airportsMap.get(movement.getOrigin()) != null) {
                context.emit(movement.getOrigin(), 1L);
            }
        }
        else {

            if(airportsMap.get(movement.getDestination()) != null) {
                context.emit(movement.getDestination(), 1L);
            }
        }
    }
}

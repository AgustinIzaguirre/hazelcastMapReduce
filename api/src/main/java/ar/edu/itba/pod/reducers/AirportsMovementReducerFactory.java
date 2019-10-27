package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class AirportsMovementReducerFactory implements ReducerFactory<String, Long, Long>{

    @Override
    public Reducer<Long, Long> newReducer(String key ) {
        return new AirportsMovementReducer();
    }
    private class AirportsMovementReducer extends Reducer<Long, Long> {
        private static final long serialVersionUID = 1L;
        private volatile long sum;

        @Override
        public void beginReduce () {
            sum = 0;
        }

        @Override
        public void reduce( Long value ) {
            sum += value.longValue();
        }

        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }
}

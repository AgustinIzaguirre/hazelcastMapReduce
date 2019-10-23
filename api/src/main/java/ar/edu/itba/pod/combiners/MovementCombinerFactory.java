package ar.edu.itba.pod.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

public class MovementCombinerFactory  implements CombinerFactory<String, Long, Long> {
    @Override
    public Combiner<Long, Long> newCombiner( String key ) {
        return new MovementCombiner();
    }

    private class MovementCombiner extends Combiner<Long, Long> {
        private long sum = 0;

        @Override
        public void combine(Long value) {
            sum++;
        }
        @Override
        public Long finalizeChunk() {
            return sum;
        }
        @
                Override
        public void reset() {
            sum = 0;
        }
    }

}

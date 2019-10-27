package ar.edu.itba.pod.combiners;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.LinkedList;
import java.util.List;

public class SameThousandCombinerFactory implements CombinerFactory<Long, List<String>, List<String>> {
    private static final long serialVersionUID = 1L;

    @Override
    public Combiner<List<String>, List<String>> newCombiner(Long key) {
        return new SameThousandCombiner();
    }

    private class SameThousandCombiner extends Combiner<List<String>, List<String>> {
        private List<String> codeList;

        public SameThousandCombiner() {
            codeList = new LinkedList<>();
        }

        @Override
        public void combine(List<String> values) {
            values.forEach(value -> codeList.add(value));
        }

        @Override
        public List<String> finalizeChunk() {
            return codeList;
        }

        @Override
        public void reset() {
            codeList = new LinkedList<>();
        }
    }
}
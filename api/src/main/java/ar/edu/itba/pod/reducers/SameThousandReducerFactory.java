package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.LinkedList;
import java.util.List;

public class SameThousandReducerFactory implements ReducerFactory<Long, List<String>, List<String>> {
    private static final long serialVersionUID = 1L;

    @Override
    public Reducer<List<String>, List<String>> newReducer(Long key){
            return new SameThousandReducer();
            }

    private class SameThousandReducer extends Reducer<List<String>, List<String>> {
        private volatile List<String> codeList;

        @Override
        public void beginReduce() {
            codeList = new LinkedList<>();
        }

        @Override
        public void reduce(List<String> values) {
            values.forEach(value -> codeList.add(value));
        }

        @Override
        public List<String> finalizeReduce() {
            return codeList;
        }
    }
}

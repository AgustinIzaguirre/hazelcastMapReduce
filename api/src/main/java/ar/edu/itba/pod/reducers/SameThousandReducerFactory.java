package ar.edu.itba.pod.reducers;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.LinkedList;
import java.util.List;

public class SameThousandReducerFactory implements ReducerFactory<Long, String, List<String>> {

    @Override
    public Reducer<String, List<String>> newReducer(Long key){
            return new SameThousandReducer();
            }

    private class SameThousandReducer extends Reducer<String, List<String>> {
        private volatile List<String> codeList;

        @Override
        public void beginReduce() {
            codeList = new LinkedList<>();
        }

        @Override
        public void reduce(String value) {
            codeList.add(value);
        }

        @Override
        public List<String> finalizeReduce() {
            return codeList;
        }
    }
}

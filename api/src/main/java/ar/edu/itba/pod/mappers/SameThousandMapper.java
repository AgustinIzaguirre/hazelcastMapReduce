package ar.edu.itba.pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.LinkedList;
import java.util.List;

public class SameThousandMapper implements Mapper<String, Long, Long, List<String>> {
    private static final long serialVersionUID = 1L;

    @Override
    public void map(String code, Long movements, Context<Long, List<String>> context) {
        if(movements >= 1000) {
            List<String> result = new LinkedList<>();
            result.add(code);
            long thousand = (movements / 1000) * 1000;
            context.emit(thousand, result);
        }
    }
}

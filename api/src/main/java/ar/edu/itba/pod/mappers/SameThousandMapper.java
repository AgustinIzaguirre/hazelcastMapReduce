package ar.edu.itba.pod.mappers;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class SameThousandMapper implements Mapper<String, Long, Long, String> {


    @Override
    public void map(String code, Long movements, Context<Long, String> context) {
        if(movements >= 1000) {
            long thousand = (movements / 1000) * 1000;
            context.emit(thousand, code);
        }
    }
}

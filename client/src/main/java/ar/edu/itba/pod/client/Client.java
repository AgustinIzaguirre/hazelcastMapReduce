package ar.edu.itba.pod.client;

import ar.edu.itba.pod.mappers.TokenizerMapper;
import ar.edu.itba.pod.models.Airport;
import ar.edu.itba.pod.reducers.WordCountReducerFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        FileLoader fileLoader = new FileLoader();
        final ClientConfig config = new XmlClientConfigBuilder("hazelcast.xml").build();
        final HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(config);
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("aeropuertos");

        fileLoader.loadAirports("aeropuertos.csv", airportsMap);
        //load movements

//        final IMap<String, String> map = hazelcastInstance.getMap("libros");
//        final KeyValueSource<String, String> source = KeyValueSource.fromMap(map);
//
//        JobTracker jobTracker = hazelcastInstance.getJobTracker("word-count");
//
//        Job<String, String> job = jobTracker.newJob(source);
//        ICompletableFuture<Map<String, Long>> future = job
//                .mapper(new TokenizerMapper())
//                .reducer(new WordCountReducerFactory())
//                .submit();
//        // Wait and retrieve the result
//        Map<String, Long> result = future.get();
//        System.out.println("hola: " + result.get("hola"));
//        System.out.println("como: " + result.get("como"));
//        System.out.println("va: " + result.get("va"));
//        System.out.println("que: " + result.get("que"));
//        System.out.println("tal: " + result.get("tal"));
//        System.out.println("mundo: " + result.get("mundo"));

    }

}

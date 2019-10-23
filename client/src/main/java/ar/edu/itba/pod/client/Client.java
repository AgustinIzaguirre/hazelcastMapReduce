package ar.edu.itba.pod.client;

import ar.edu.itba.pod.collators.OaciAirportsMovementCollator;
import ar.edu.itba.pod.mappers.CabotageFlightsMapper;
import ar.edu.itba.pod.mappers.DestinationAirportMapper;
import ar.edu.itba.pod.mappers.OaciAirportsMovementMapper;
import ar.edu.itba.pod.mappers.TokenizerMapper;
import ar.edu.itba.pod.models.Airport;
import ar.edu.itba.pod.models.Movement;
import ar.edu.itba.pod.reducers.AirportsMovementReducerFactory;
import ar.edu.itba.pod.reducers.WordCountReducerFactory;
import ar.edu.itba.pod.results.AirportsMovementResult;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        FileLoader fileLoader = new FileLoader();
        final ClientConfig config = new XmlClientConfigBuilder("hazelcast.xml").build();
        final HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(config);
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("aeropuertos");
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("movimientos");
        //clear maps
        airportsMap.clear();
        movementsMap.clear();
        //load maps
        fileLoader.loadAirports("aeropuertos.csv", airportsMap);
        fileLoader.loadMovements("movimientos.csv", movementsMap);
        int queryNumber = 1;//TODO get from params
        solveQuery(queryNumber, hazelcastInstance, airportsMap, movementsMap);

    }

    private static void solveQuery(int queryNumber, HazelcastInstance hazelcastInstance,
                                   IMap<String, Airport> airportsMap, IMap<Long, Movement> movementsMap)
                                    throws ExecutionException, InterruptedException, IOException {
        switch (queryNumber) {
            case 1:
                airportsMovementQuery(hazelcastInstance, airportsMap, movementsMap);
                break;
            case 2:
                cabotagePercentage(hazelcastInstance, airportsMap, movementsMap);

                break;
            case 3:
                destinationAirports(hazelcastInstance, airportsMap, movementsMap);
                break;
            case 4:
                break;
        }
    }

    private static void airportsMovementQuery(HazelcastInstance hazelcastInstance, IMap<String, Airport> airportsMap,
                                              IMap<Long, Movement> movementsMap) throws ExecutionException, InterruptedException, IOException {
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);

        JobTracker jobTracker = hazelcastInstance.getJobTracker("query-1");

        Job<Long, Movement> job = jobTracker.newJob(source);
        ICompletableFuture<List<AirportsMovementResult>> future = job
                .mapper(new OaciAirportsMovementMapper())
                .reducer(new AirportsMovementReducerFactory())
                .submit(new OaciAirportsMovementCollator());
        List<AirportsMovementResult> result = future.get();
        ResultWriter.writeResult1("output1.csv", result, airportsMap);

    }

    private static void cabotagePercentage(HazelcastInstance hazelcastInstance, IMap<String, Airport> airportsMap,
                                           IMap<Long, Movement> movementsMap) throws ExecutionException, InterruptedException {
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        JobTracker jobTracker = hazelcastInstance.getJobTracker("query-2");
        Job<Long, Movement> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new CabotageFlightsMapper())
                .reducer(new AirportsMovementReducerFactory())
                .submit();
        Map<String, Long> result = future.get();

    }

    private static void destinationAirports(HazelcastInstance hazelcastInstance, IMap<String, Airport> airportsMap,
                                            IMap<Long, Movement> movementsMap) throws ExecutionException, InterruptedException {
        String specifiedOaci = "SACO";//TODO replace with param
        JobTracker jobTracker = hazelcastInstance.getJobTracker("query-4");
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        Job<Long, Movement> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new DestinationAirportMapper(specifiedOaci))
                .reducer(new AirportsMovementReducerFactory())
                .submit();
        Map<String, Long> result = future.get();

    }

}

package ar.edu.itba.pod;

import ar.edu.itba.pod.Util.ResultComparator;
import ar.edu.itba.pod.client.ResultWriter;
import ar.edu.itba.pod.collators.OaciAirportsMovementCollator;
import ar.edu.itba.pod.mappers.OaciAirportsMovementMapper;
import ar.edu.itba.pod.models.Airport;
import ar.edu.itba.pod.models.Movement;
import ar.edu.itba.pod.reducers.AirportsMovementReducerFactory;
import ar.edu.itba.pod.results.AirportsMovementResult;
import ar.edu.itba.pod.server.Server;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class QueryOneTest {

    private static HazelcastInstance hazelcastInstance;


    @BeforeClass
    public static void startServer() throws IOException {
        Server.startServer("src/test/data/hazelcast.xml");
        hazelcastInstance = createClient();
    }

    private static HazelcastInstance createClient() throws IOException {
        ClientConfig config = new XmlClientConfigBuilder("src/test/data/hazelcast.xml").build();
        return HazelcastClient.newHazelcastClient(config);
    }

    @Test
    public void emptyQueryResultTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("g12-aeropuertos");
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        JobTracker jobTracker = hazelcastInstance.getJobTracker("query-1");
        Job<Long, Movement> job = jobTracker.newJob(source);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery1Result.csv";

        //Action
        ICompletableFuture<List<AirportsMovementResult>> future = job
                .mapper(new OaciAirportsMovementMapper())   //TODO add combiner maybe
                .reducer(new AirportsMovementReducerFactory())
                .submit(new OaciAirportsMovementCollator());
        List<AirportsMovementResult> result = future.get();
        ResultWriter.writeResult1("src/test/data/results/answer.csv", result, airportsMap);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }
}

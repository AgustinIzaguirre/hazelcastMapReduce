package ar.edu.itba.pod;

import ar.edu.itba.pod.Util.ResultComparator;
import ar.edu.itba.pod.client.Client;
import ar.edu.itba.pod.client.FileLoader;
import ar.edu.itba.pod.models.Airport;
import ar.edu.itba.pod.models.Movement;
import ar.edu.itba.pod.server.Server;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.IOException;
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

    private static void loadMaps(IMap<String, Airport> airportsMap, IMap<Long, Movement> movementIMap,
                                 String airportPath, String movementPath) throws IOException {
        airportsMap.clear();
        movementIMap.clear();
        FileLoader fileLoader = new FileLoader();
        fileLoader.loadAirports(airportPath, airportsMap);
        fileLoader.loadMovements(movementPath, movementIMap);
    }

    @Test
    public void emptyQueryResultWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("g12-aeropuertos");
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        airportsMap.clear();
        movementsMap.clear();
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery1Result.csv";

        //Action
        Client.airportsMovementQuery(hazelcastInstance, airportsMap, movementsMap, false, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void emptyQueryResultWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("g12-aeropuertos");
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        airportsMap.clear();
        movementsMap.clear();
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery1Result.csv";

        //Action
        Client.airportsMovementQuery(hazelcastInstance, airportsMap, movementsMap, true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void emptyQueryResultWithAwareAndWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("g12-aeropuertos");
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        airportsMap.clear();
        movementsMap.clear();
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery1Result.csv";

        //Action
        Client.airportsMovementQueryWithAware(hazelcastInstance, airportsMap, movementsMap, false, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void emptyQueryResultWithAwareAndWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("g12-aeropuertos");
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        airportsMap.clear();
        movementsMap.clear();
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery1Result.csv";

        //Action
        Client.airportsMovementQueryWithAware(hazelcastInstance, airportsMap, movementsMap, true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void fullQueryResultWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("g12-aeropuertos");
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String airportPath = "src/test/data/aeropuertos.csv";
        String movementPath = "src/test/data/movimientos.csv";
        loadMaps(airportsMap, movementsMap, airportPath, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/query1.csv";

        //Action
        Client.airportsMovementQuery(hazelcastInstance, airportsMap, movementsMap, false, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void fullQueryResultWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("g12-aeropuertos");
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String airportPath = "src/test/data/aeropuertos.csv";
        String movementPath = "src/test/data/movimientos.csv";
        loadMaps(airportsMap, movementsMap, airportPath, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/query1.csv";

        //Action
        Client.airportsMovementQuery(hazelcastInstance, airportsMap, movementsMap, true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void fullQueryResultWithAwareAndWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("g12-aeropuertos");
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String airportPath = "src/test/data/aeropuertos.csv";
        String movementPath = "src/test/data/movimientos.csv";
        loadMaps(airportsMap, movementsMap, airportPath, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/query1.csv";

        //Action
        Client.airportsMovementQueryWithAware(hazelcastInstance, airportsMap, movementsMap, false, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void fullQueryResultWithAwareAndWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("g12-aeropuertos");
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String airportPath = "src/test/data/aeropuertos.csv";
        String movementPath = "src/test/data/movimientos.csv";
        loadMaps(airportsMap, movementsMap, airportPath, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/query1.csv";

        //Action
        Client.airportsMovementQueryWithAware(hazelcastInstance, airportsMap, movementsMap, true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    //TODO add one adhoc case and test with the 4 possible combinations

}

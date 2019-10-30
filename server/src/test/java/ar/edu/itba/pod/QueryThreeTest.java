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

public class QueryThreeTest {

    private static HazelcastInstance hazelcastInstance;


    @BeforeClass
    public static void startServer() throws IOException {
        Server.startServer("src/test/data/hazelcast.xml");
        hazelcastInstance = createClient(); //TODO maybbe singleton so as not to reload for every class
    }

    private static HazelcastInstance createClient() throws IOException {
        ClientConfig config = new XmlClientConfigBuilder("src/test/data/hazelcast.xml").build();
        return HazelcastClient.newHazelcastClient(config); //TODO maybbe singleton so as not to reload for every class
    }

    private static void loadMaps(IMap<Long, Movement> movementMap, String movementPath) throws IOException {
        movementMap.clear();
        FileLoader fileLoader = new FileLoader();
        fileLoader.loadMovements(movementPath, movementMap);
    }

    @Test
    public void emptyQueryResultWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        movementsMap.clear();
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery3Result.csv";

        //Action
        Client.pairAirportsWithSameThousands(hazelcastInstance, movementsMap, false, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void emptyQueryResultWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        movementsMap.clear();
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery3Result.csv";

        //Action
        Client.pairAirportsWithSameThousands(hazelcastInstance, movementsMap, true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void emptyQueryResultWithSecondMapReduceWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        movementsMap.clear();
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery3Result.csv";

        //Action
        Client.pairAirportsWithSameThousandsWithSecondMapReduce(hazelcastInstance, movementsMap, false, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void emptyQueryResultWithSecondMapReduceWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        movementsMap.clear();
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery3Result.csv";

        //Action
        Client.pairAirportsWithSameThousandsWithSecondMapReduce(hazelcastInstance, movementsMap, true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void emptyQueryResultWithPairingInReducerWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        movementsMap.clear();
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery3Result.csv";

        //Action
        Client.pairAirportsWithSameThousandsPairInReducer(hazelcastInstance, movementsMap, false, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void emptyQueryResultWithPairingInReducerWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        movementsMap.clear();
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery3Result.csv";

        //Action
        Client.pairAirportsWithSameThousandsPairInReducer(hazelcastInstance, movementsMap, true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void fullQueryResultWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientos.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/query3.csv";

        //Action
        Client.pairAirportsWithSameThousands(hazelcastInstance, movementsMap, false, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void fullQueryResultWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientos.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/query3.csv";

        //Action
        Client.pairAirportsWithSameThousands(hazelcastInstance, movementsMap, true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void fullQueryResultWithSecondMapReduceWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientos.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/query3.csv";

        //Action
        Client.pairAirportsWithSameThousandsWithSecondMapReduce(hazelcastInstance, movementsMap, false, resultPath);
        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void fullQueryResultWithSecondMapReduceWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientos.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/query3.csv";

        //Action
        Client.pairAirportsWithSameThousandsWithSecondMapReduce(hazelcastInstance, movementsMap, true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void fullQueryResultWithPairingInReducerWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientos.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/query3.csv";

        //Action
        Client.pairAirportsWithSameThousandsPairInReducer(hazelcastInstance, movementsMap, false, resultPath);
        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void fullQueryResultWithPairingInReducerWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientos.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/query3.csv";

        //Action
        Client.pairAirportsWithSameThousandsPairInReducer(hazelcastInstance, movementsMap, true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void onlyOneAirportWithThousandWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientosQuery3.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery3Result.csv";

        //Action
        Client.pairAirportsWithSameThousands(hazelcastInstance, movementsMap, false, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void onlyOneAirportWithThousandWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientosQuery3.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery3Result.csv";

        //Action
        Client.pairAirportsWithSameThousands(hazelcastInstance, movementsMap, true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void onlyOneAirportWithThousandWithSecondMapReduceWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientosQuery3.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery3Result.csv";

        //Action
        Client.pairAirportsWithSameThousandsWithSecondMapReduce(hazelcastInstance, movementsMap, false, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void onlyOneAirportWithThousandWithSecondMapReduceWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientosQuery3.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery3Result.csv";

        //Action
        Client.pairAirportsWithSameThousandsWithSecondMapReduce(hazelcastInstance, movementsMap, true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void onlyOneAirportWithPairingInReducerWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientosQuery3.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery3Result.csv";

        //Action
        Client.pairAirportsWithSameThousandsPairInReducer(hazelcastInstance, movementsMap, false, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void onlyOneAirportWithPairingInReducerWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientosQuery3.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery3Result.csv";

        //Action
        Client.pairAirportsWithSameThousandsPairInReducer(hazelcastInstance, movementsMap, true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

}

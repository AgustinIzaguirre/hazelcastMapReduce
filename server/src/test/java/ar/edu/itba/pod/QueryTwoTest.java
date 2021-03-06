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

public class QueryTwoTest {

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

    private static void loadMaps(IMap<Long, Movement> movementIMap, String movementPath) throws IOException {
        movementIMap.clear();
        FileLoader fileLoader = new FileLoader();
        fileLoader.loadMovements(movementPath, movementIMap);
    }

    @Test
    public void emptyQueryResultWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        movementsMap.clear();
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery2Result.csv";

        //Action
        Client.cabotagePercentage(hazelcastInstance, movementsMap, 4,false, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void emptyQueryResultWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        movementsMap.clear();
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/emptyQuery2Result.csv";

        //Action
        Client.cabotagePercentage(hazelcastInstance, movementsMap, 4,true, resultPath);

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
        String expectedPath = "src/test/data/results/expectedResults/query2.csv";

        //Action
        Client.cabotagePercentage(hazelcastInstance, movementsMap, 5,false, resultPath);

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
        String expectedPath = "src/test/data/results/expectedResults/query2.csv";

        //Action
        Client.cabotagePercentage(hazelcastInstance, movementsMap, 5,true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void lessResultsThanAskedForWithoutCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientosQuery2.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/test1Query2Result.csv";

        //Action
        Client.cabotagePercentage(hazelcastInstance, movementsMap, 5,false, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

    @Test
    public void lessResultsThanAskedForWithCombinerTest() throws IOException, ExecutionException, InterruptedException {
        //Set up
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        String movementPath = "src/test/data/movimientosQuery2.csv";
        loadMaps(movementsMap, movementPath);
        String resultPath = "src/test/data/results/answer.csv";
        String expectedPath = "src/test/data/results/expectedResults/test1Query2Result.csv";

        //Action
        Client.cabotagePercentage(hazelcastInstance, movementsMap, 5,true, resultPath);

        //Results
        Assert.assertTrue(ResultComparator.compareFiles(expectedPath, resultPath));
    }

}

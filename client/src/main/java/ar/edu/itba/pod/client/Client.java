package ar.edu.itba.pod.client;

import ar.edu.itba.pod.collators.CabotageFlightsCollator;
import ar.edu.itba.pod.collators.DestinationAirportCollator;
import ar.edu.itba.pod.collators.OaciAirportsMovementCollator;
import ar.edu.itba.pod.collators.PairAirportCollator;
import ar.edu.itba.pod.combiners.MovementCombinerFactory;
import ar.edu.itba.pod.mappers.CabotageFlightsMapper;
import ar.edu.itba.pod.mappers.DestinationAirportMapper;
import ar.edu.itba.pod.mappers.OaciAirportsMovementMapper;
import ar.edu.itba.pod.models.Airport;
import ar.edu.itba.pod.models.Movement;
import ar.edu.itba.pod.reducers.AirportsMovementReducerFactory;
import ar.edu.itba.pod.results.AirportPairResult;
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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);
    private static FileWriter timeFile;
    private static BufferedWriter timeFileWriter;
    private static int queryNumber;
    private static String[] addresses;
    private static String outputDirectoryPath = ".";
    private static String inputDirectoryPath = ".";
    private static String airportsFilePath = ".";
    private static String movementFilePath = ".";
    private static String resultFilePath;
    private static String timeFilePath;
    private static long quantity;
    private static String originOaci;

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        queryNumber = 1;//TODO get from params
        loadProperties();
        ClientConfig config = loadClientConfig();
        final HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(config);
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("g12-aeropuertos");
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        loadData(airportsMap, movementsMap);
        solveQuery(queryNumber, hazelcastInstance, airportsMap, movementsMap);
        timeFileWriter.close();
        System.out.println("Finished\n");
    }

    private static void loadProperties() {
//        queryNumber = Integer.parseInt(System.getProperty("queryNumber"));  // TODO use this on production
//        addresses= System.getProperty("addresses").split(";");  // TODO use this on production
//        inputDirectoryPath = System.getProperty("inPath");    // TODO use this on production
//        outputDirectoryPath = System.getProperty("outPath");  // TODO use this on production
        airportsFilePath = inputDirectoryPath + "/aeropuertos.csv";
        movementFilePath = inputDirectoryPath + "/movimientos.csv";
//        resultFilePath = outputDirectoryPath + "/query" + queryNumber + ".csv"; // TODO use this on production

        resultFilePath = outputDirectoryPath + "/query" + queryNumber + "_v2.csv";
        timeFilePath = outputDirectoryPath + "/query" + queryNumber + ".txt";
        if(queryNumber < 1 || queryNumber > 4) {
            //TODO throw exception
        }
//        if(queryNumber == 2 || queryNumber == 4) {
//            quantity = Long.parseLong(System.getProperty("n"));
//            if(queryNumber == 2) {
//                originOaci = System.getProperty("originOaci");
//            }
//        } //TODO use this on production
    }

    private static ClientConfig loadClientConfig() throws IOException {
        final ClientConfig config = new XmlClientConfigBuilder("hazelcast.xml").build();//TODO update with ips
//        config.getNetworkConfig().addAddresses(addresses); //TODO add in production and set to addd using list instead of array
        return config;
    }

    private static void loadData(IMap<String, Airport> airportsMap, IMap<Long, Movement> movementsMap) throws IOException {
        //set time file parameters
        timeFile = new FileWriter(timeFilePath) ;//TODO replace with param
        timeFileWriter = new BufferedWriter(timeFile);
        //clear maps
        airportsMap.clear();
        movementsMap.clear();
        //load maps
        FileLoader fileLoader = new FileLoader();
        ResultWriter.writeTime(timeFileWriter, "Inicio de la lectura del archivo");
        fileLoader.loadAirports("aeropuertos.csv", airportsMap);
        fileLoader.loadMovements("movimientos.csv", movementsMap);
        ResultWriter.writeTime(timeFileWriter, "Fin de lectura del archivo");
    }

    private static void solveQuery(int queryNumber, HazelcastInstance hazelcastInstance,
                                   IMap<String, Airport> airportsMap, IMap<Long, Movement> movementsMap)
                                    throws ExecutionException, InterruptedException, IOException {
        ResultWriter.writeTime(timeFileWriter, "Inicio del trabajo map/reduce");
        boolean useCombiner = true;//TODO generalize
        switch (queryNumber) {
            case 1:
                airportsMovementQuery(hazelcastInstance, airportsMap, movementsMap, useCombiner);
                break;
            case 2:
                cabotagePercentage(hazelcastInstance, movementsMap);
                break;
            case 3:
                pairAirportsWithSameThousands(hazelcastInstance, movementsMap);
//                pairAirportsWithSameThousandsWithSecondMapReduce(hazelcastInstance, movementsMap);
                break;
            case 4:
                destinationAirports(hazelcastInstance, movementsMap);
                break;
        }
        ResultWriter.writeTime(timeFileWriter, "Fin del trabajo map/reduce");

    }



    private static void airportsMovementQuery(HazelcastInstance hazelcastInstance, IMap<String, Airport> airportsMap,
                                              IMap<Long, Movement> movementsMap, boolean useCombiner)
                                                        throws ExecutionException, InterruptedException, IOException {
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);

        JobTracker jobTracker = hazelcastInstance.getJobTracker("query-1");

        Job<Long, Movement> job = jobTracker.newJob(source);
        List<AirportsMovementResult> result = null;
        if(useCombiner) {
            ICompletableFuture<List<AirportsMovementResult>> future = job
                    .mapper(new OaciAirportsMovementMapper())
                    .combiner(new MovementCombinerFactory())
                    .reducer(new AirportsMovementReducerFactory())
                    .submit(new OaciAirportsMovementCollator());
                    result = future.get();
        }
        else {
            ICompletableFuture<List<AirportsMovementResult>> future = job
                    .mapper(new OaciAirportsMovementMapper())
                    .reducer(new AirportsMovementReducerFactory())
                    .submit(new OaciAirportsMovementCollator());
                    result = future.get();
        }
        ResultWriter.writeResult1(resultFilePath, result, airportsMap);
    }

    private static void cabotagePercentage(HazelcastInstance hazelcastInstance, IMap<Long, Movement> movementsMap)
                                                        throws ExecutionException, InterruptedException, IOException {
        long quantity = 5;
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        JobTracker jobTracker = hazelcastInstance.getJobTracker("query-2");
        Job<Long, Movement> job = jobTracker.newJob(source);
        ICompletableFuture<List<AirportsMovementResult>> future = job
                .mapper(new CabotageFlightsMapper())    //TODO add combiner maybe
                .reducer(new AirportsMovementReducerFactory())
                .submit(new CabotageFlightsCollator(quantity));
        List<AirportsMovementResult> result = future.get();
        ResultWriter.writeResult2(resultFilePath, result);
    }

    private static void pairAirportsWithSameThousands(HazelcastInstance hazelcastInstance,
                                                      IMap<Long, Movement> movementsMap)
                                            throws ExecutionException, InterruptedException, IOException {
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        JobTracker jobTracker = hazelcastInstance.getJobTracker("query-3");
        Job<Long, Movement> job = jobTracker.newJob(source);
        ICompletableFuture<List<AirportPairResult>> future = job
                .mapper(new OaciAirportsMovementMapper())  //TODO add combiner maybe
                .reducer(new AirportsMovementReducerFactory())
                .submit(new PairAirportCollator());
       List<AirportPairResult> resultList = future.get();
       ResultWriter.writeResult3(resultFilePath, resultList);
    }

    private static void pairAirportsWithSameThousandsWithSecondMapReduce(HazelcastInstance hazelcastInstance, IMap<Long,
                                                                            Movement> movementsMap)
                                                                            throws ExecutionException, InterruptedException {

        //first MapReduce
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        JobTracker jobTracker = hazelcastInstance.getJobTracker("preCalculation");
        Job<Long, Movement> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new OaciAirportsMovementMapper())
                .reducer(new AirportsMovementReducerFactory())
                .submit();
        Map<String, Long> resultMap = future.get();

        //second MapReduce
        String mapName = "g12-airportMovementMap";
        IMap<String, Long> airportMovementMap = loadMap(hazelcastInstance, resultMap, mapName);
        final KeyValueSource<String, Long> secondSource = KeyValueSource.fromMap(airportMovementMap);
        jobTracker = hazelcastInstance.getJobTracker("query-3");
        Job<String, Long> secondJob = jobTracker.newJob(secondSource);
//        ICompletableFuture<Map<String, Long>> future = job
//                .mapper(new OaciAirportsMovementMapper())
//                .reducer(new AirportsMovementReducerFactory())
//                .submit();
//        Map<String, Long> airportMovementMap = future.get();
//

//        ResultWriter.writeResult1("output1.csv", result, airportsMap);
        //TODO
        //clear map
        airportMovementMap.clear();

    }

    private static IMap<String,Long> loadMap(HazelcastInstance hazelcastInstance, Map<String, Long> resultMap, String mapName) {
        IMap<String, Long> airportMovementMap = hazelcastInstance.getMap(mapName);
        resultMap.forEach((key, value)-> airportMovementMap.put(key, value));
        return airportMovementMap;
    }


    private static void destinationAirports(HazelcastInstance hazelcastInstance, IMap<Long, Movement> movementsMap)
                                                        throws ExecutionException, InterruptedException, IOException {
        String specifiedOaci = "SAEZ";//TODO replace with param
        long quantity = 5; //TODO replace with param
        JobTracker jobTracker = hazelcastInstance.getJobTracker("query-4");
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        Job<Long, Movement> job = jobTracker.newJob(source);
        ICompletableFuture<List<AirportsMovementResult>> future = job
                .mapper(new DestinationAirportMapper(specifiedOaci))    //TODO add combiner maybe
                .reducer(new AirportsMovementReducerFactory())
                .submit(new DestinationAirportCollator(quantity));
        List<AirportsMovementResult> result = future.get();
        ResultWriter.writeResult4(resultFilePath, result);
    }

}

package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.exceptions.InvalidQueryException;
import ar.edu.itba.pod.client.exceptions.RequiredPropertyException;
import ar.edu.itba.pod.collators.*;
import ar.edu.itba.pod.combiners.MovementCombinerFactory;
import ar.edu.itba.pod.combiners.SameThousandCombinerFactory;
import ar.edu.itba.pod.mappers.*;
import ar.edu.itba.pod.models.Airport;
import ar.edu.itba.pod.models.Movement;
import ar.edu.itba.pod.reducers.AirportsMovementReducerFactory;
import ar.edu.itba.pod.reducers.SameThousandReducerFactory;
import ar.edu.itba.pod.reducers.SameThousandWithPairReducerFactory;
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
import java.util.*;
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

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException,
                                                        RequiredPropertyException, InvalidQueryException {

        loadProperties();
        ClientConfig config = loadClientConfig();
        final HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(config);
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("g12-aeropuertos");
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        loadData(airportsMap, movementsMap);
        solveQuery(queryNumber, hazelcastInstance, airportsMap, movementsMap);
        timeFileWriter.close();
        hazelcastInstance.shutdown();
    }

    private static void loadProperties() throws RequiredPropertyException, InvalidQueryException {
        Optional<String> addressesProperty = Optional.ofNullable(System.getProperty("addresses"));
        Optional<String> inPathProperty = Optional.ofNullable(System.getProperty("inPath"));
        Optional<String> outPathProperty = Optional.ofNullable(System.getProperty("outPath"));
        queryNumber = Integer.parseInt(System.getProperty("queryNumber"));
        addresses= addressesProperty.orElseThrow(() -> new RequiredPropertyException("addresses property is required.")).split(";");
        inputDirectoryPath = inPathProperty.orElseThrow(() -> new RequiredPropertyException("inPath property is required."));
        outputDirectoryPath = outPathProperty.orElseThrow(() -> new RequiredPropertyException("outPath property is required."));
        airportsFilePath = inputDirectoryPath + "/aeropuertos.csv";
        movementFilePath = inputDirectoryPath + "/movimientos.csv";
        resultFilePath = outputDirectoryPath + "/query" + queryNumber + ".csv";
        timeFilePath = outputDirectoryPath + "/query" + queryNumber + ".txt";

        if(queryNumber < 1 || queryNumber > 4) {
            throw new InvalidQueryException("Invalid query number.Query number should be 1,2,3 or 4.");// should never happen
        }

        if(queryNumber == 2 || queryNumber == 4) {
            Optional<String> quantityProperty = Optional.ofNullable(System.getProperty("n"));
            quantity = Long.parseLong(quantityProperty.orElseThrow(() -> new RequiredPropertyException("n property is required.")));

            if(queryNumber == 4) {
                Optional<String> oaciProperty = Optional.ofNullable(System.getProperty("oaci"));
                originOaci = oaciProperty.orElseThrow(() -> new RequiredPropertyException("oaci property is required."));
            }
        }
    }

    private static ClientConfig loadClientConfig() throws IOException {
        final ClientConfig config = new XmlClientConfigBuilder("hazelcast-client.xml").build();
        List<String> newAddresses = loadAddresses();
        config.getNetworkConfig().setAddresses(newAddresses);
        return config;
    }

    private static List<String> loadAddresses() {
        List<String> newAddresses = new LinkedList<>();

        for(int i = 0; i < addresses.length; i++) {
            newAddresses.add(addresses[i]);
        }

        return newAddresses;
    }

    private static void loadData(IMap<String, Airport> airportsMap, IMap<Long, Movement> movementsMap) throws IOException {
        //set time file parameters
        timeFile = new FileWriter(timeFilePath) ;
        timeFileWriter = new BufferedWriter(timeFile);
        //clear maps
        airportsMap.clear();
        movementsMap.clear();
        //load maps
        FileLoader fileLoader = new FileLoader();
        ResultWriter.writeTime(timeFileWriter, "Inicio de la lectura del archivo");

        if(queryNumber == 1) {
            fileLoader.loadAirports(airportsFilePath, airportsMap);
        }

        fileLoader.loadMovements(movementFilePath, movementsMap);
        ResultWriter.writeTime(timeFileWriter, "Fin de lectura del archivo");
    }

    private static void solveQuery(int queryNumber, HazelcastInstance hazelcastInstance,
                                   IMap<String, Airport> airportsMap, IMap<Long, Movement> movementsMap)
                                    throws ExecutionException, InterruptedException, IOException {
        ResultWriter.writeTime(timeFileWriter, "Inicio del trabajo map/reduce");

        switch (queryNumber) {
            case 1:
                airportsMovementQuery(hazelcastInstance, airportsMap, movementsMap, true, resultFilePath);
//                airportsMovementQueryWithAware(hazelcastInstance, airportsMap, movementsMap, true, resultFilePath);
                break;

            case 2:
                cabotagePercentage(hazelcastInstance, movementsMap, quantity,true, resultFilePath);
                break;

            case 3:
                pairAirportsWithSameThousands(hazelcastInstance, movementsMap, true, resultFilePath);
//                pairAirportsWithSameThousandsWithSecondMapReduce(hazelcastInstance, movementsMap,true, resultFilePath);
//                pairAirportsWithSameThousandsPairInReducer(hazelcastInstance, movementsMap,true, resultFilePath);

                break;

            case 4:
                destinationAirports(hazelcastInstance, movementsMap, originOaci, quantity, true, resultFilePath);
                break;
        }

        ResultWriter.writeTime(timeFileWriter, "Fin del trabajo map/reduce");
    }



    public static void airportsMovementQuery(HazelcastInstance hazelcastInstance, IMap<String, Airport> airportsMap,
                                              IMap<Long, Movement> movementsMap, boolean useCombiner, String resultPath)
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

        ResultWriter.writeResult1(resultPath, result, airportsMap);
    }

    public static void airportsMovementQueryWithAware(HazelcastInstance hazelcastInstance, IMap<String, Airport> airportsMap,
                                             IMap<Long, Movement> movementsMap, boolean useCombiner, String resultPath)
            throws ExecutionException, InterruptedException, IOException {
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        OaciAirportsMovementWithInstanceAwareMapper mapper = new OaciAirportsMovementWithInstanceAwareMapper();
        mapper.setHazelcastInstance(hazelcastInstance);
        JobTracker jobTracker = hazelcastInstance.getJobTracker("query-1");
        Job<Long, Movement> job = jobTracker.newJob(source);
        List<AirportsMovementResult> result = null;

        if(useCombiner) {
            ICompletableFuture<List<AirportsMovementResult>> future = job
                    .mapper(mapper)
                    .combiner(new MovementCombinerFactory())
                    .reducer(new AirportsMovementReducerFactory())
                    .submit(new OaciAirportsMovementCollator());
            result = future.get();
        }
        else {
            ICompletableFuture<List<AirportsMovementResult>> future = job
                    .mapper(mapper)
                    .reducer(new AirportsMovementReducerFactory())
                    .submit(new OaciAirportsMovementCollator());
            result = future.get();
        }

        ResultWriter.writeResult1WithAware(resultPath, result, airportsMap);
    }

    public static void cabotagePercentage(HazelcastInstance hazelcastInstance, IMap<Long, Movement> movementsMap,
                                                            long quantity, boolean useCombiner, String resultPath)
                                                        throws ExecutionException, InterruptedException, IOException {
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        JobTracker jobTracker = hazelcastInstance.getJobTracker("query-2");
        Job<Long, Movement> job = jobTracker.newJob(source);
        List<AirportsMovementResult> result = null;

        if(useCombiner) {
            ICompletableFuture<List<AirportsMovementResult>> future = job
                    .mapper(new CabotageFlightsMapper())
                    .combiner(new MovementCombinerFactory())
                    .reducer(new AirportsMovementReducerFactory())
                    .submit(new CabotageFlightsCollator(quantity));
                    result = future.get();
        }
        else {
            ICompletableFuture<List<AirportsMovementResult>> future = job
                    .mapper(new CabotageFlightsMapper())
                    .reducer(new AirportsMovementReducerFactory())
                    .submit(new CabotageFlightsCollator(quantity));
                    result = future.get();
        }

        ResultWriter.writeResult2(resultPath, result);
    }


    public static void pairAirportsWithSameThousands(HazelcastInstance hazelcastInstance,
                                                     IMap<Long, Movement> movementsMap,
                                                     boolean useCombiner, String resultPath)
            throws ExecutionException, InterruptedException, IOException {
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        JobTracker jobTracker = hazelcastInstance.getJobTracker("query-3");
        List<AirportPairResult> resultList = null;
        Job<Long, Movement> job = jobTracker.newJob(source);

        if(useCombiner) {
            ICompletableFuture<List<AirportPairResult>> future = job
                    .mapper(new OaciAirportsMovementMapper())
                    .combiner(new MovementCombinerFactory())
                    .reducer(new AirportsMovementReducerFactory())
                    .submit(new PairAirportCollator());
            resultList = future.get();
        }
        else {
            ICompletableFuture<List<AirportPairResult>> future = job
                    .mapper(new OaciAirportsMovementMapper())
                    .reducer(new AirportsMovementReducerFactory())
                    .submit(new PairAirportCollator());
            resultList = future.get();
        }

        ResultWriter.writeResult3(resultPath, resultList);
    }

    public static void pairAirportsWithSameThousandsPairInReducer(HazelcastInstance hazelcastInstance, IMap<Long,
                                                            Movement> movementsMap, boolean useCombiner, String resultPath)
                                                                throws ExecutionException, InterruptedException, IOException {
        //first MapReduce
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        JobTracker jobTracker = hazelcastInstance.getJobTracker("preCalculation");
        Map<String, Long> resultMap = null;
        Job<Long, Movement> job = jobTracker.newJob(source);
        resultMap = makeFirstMapReduce(job, useCombiner);

        //second MapReduce
        String mapName = "g12-airportMovementMap";
        IMap<String, Long> airportMovementMap = loadMap(hazelcastInstance, resultMap, mapName);
        final KeyValueSource<String, Long> secondSource = KeyValueSource.fromMap(airportMovementMap);
        jobTracker = hazelcastInstance.getJobTracker("query-3");
        List<AirportPairResult> resultList = null;
        Job<String, Long> secondJob = jobTracker.newJob(secondSource);

        if(useCombiner) {
            ICompletableFuture<List<AirportPairResult>> secondFuture = secondJob
                    .mapper(new SameThousandMapper())
                    .combiner(new SameThousandCombinerFactory())
                    .reducer(new SameThousandWithPairReducerFactory())
                    .submit(new JoinPairListCollator());
            resultList = secondFuture.get();
        }
        else {
            ICompletableFuture<List<AirportPairResult>> secondFuture = secondJob
                    .mapper(new SameThousandMapper())
                    .reducer(new SameThousandWithPairReducerFactory())
                    .submit(new JoinPairListCollator());
            resultList = secondFuture.get();
        }

        ResultWriter.writeResult3(resultPath, resultList);

        //clear map
        airportMovementMap.clear();
    }

    private static Map<String,Long> makeFirstMapReduce(Job<Long, Movement> job, boolean useCombiner)
                                                        throws ExecutionException, InterruptedException {
        if(useCombiner) {
            ICompletableFuture<Map<String, Long>> future = job
                    .mapper(new OaciAirportsMovementMapper())
                    .combiner(new MovementCombinerFactory())
                    .reducer(new AirportsMovementReducerFactory())
                    .submit();
            return future.get();
        }
        else {
            ICompletableFuture<Map<String, Long>> future = job
                    .mapper(new OaciAirportsMovementMapper())
                    .reducer(new AirportsMovementReducerFactory())
                    .submit();
            return future.get();
        }
    }


    public static void pairAirportsWithSameThousandsWithSecondMapReduce(HazelcastInstance hazelcastInstance, IMap<Long,
            Movement> movementsMap, boolean useCombiner, String resultPath)
            throws ExecutionException, InterruptedException, IOException {
        //first MapReduce
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        JobTracker jobTracker = hazelcastInstance.getJobTracker("preCalculation");
        Map<String, Long> resultMap = null;
        Job<Long, Movement> job = jobTracker.newJob(source);
        resultMap = makeFirstMapReduce(job, useCombiner);

        //second MapReduce
        String mapName = "g12-airportMovementMap";
        IMap<String, Long> airportMovementMap = loadMap(hazelcastInstance, resultMap, mapName);
        final KeyValueSource<String, Long> secondSource = KeyValueSource.fromMap(airportMovementMap);
        jobTracker = hazelcastInstance.getJobTracker("query-3");
        List<AirportPairResult> resultList = null;
        Job<String, Long> secondJob = jobTracker.newJob(secondSource);

        if(useCombiner) {
            ICompletableFuture<List<AirportPairResult>> secondFuture = secondJob
                    .mapper(new SameThousandMapper())
                    .combiner(new SameThousandCombinerFactory())
                    .reducer(new SameThousandReducerFactory())
                    .submit(new PairSameThousandCollator());
            resultList = secondFuture.get();
        }
        else {
            ICompletableFuture<List<AirportPairResult>> secondFuture = secondJob
                    .mapper(new SameThousandMapper())
                    .reducer(new SameThousandReducerFactory())
                    .submit(new PairSameThousandCollator());
            resultList = secondFuture.get();
        }

        ResultWriter.writeResult3(resultPath, resultList);

        //clear map
        airportMovementMap.clear();
    }

    private static IMap<String,Long> loadMap(HazelcastInstance hazelcastInstance, Map<String, Long> resultMap, String mapName) {
        IMap<String, Long> airportMovementMap = hazelcastInstance.getMap(mapName);
        resultMap.forEach((key, value)-> airportMovementMap.put(key, value));
        return airportMovementMap;
    }


    public static void destinationAirports(HazelcastInstance hazelcastInstance, IMap<Long, Movement> movementsMap,
                                           String specifiedOaci, long quantity, boolean useCombiner, String resultPath)
                                                        throws ExecutionException, InterruptedException, IOException {
        JobTracker jobTracker = hazelcastInstance.getJobTracker("query-4");
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        List<AirportsMovementResult> result = null;
        Job<Long, Movement> job = jobTracker.newJob(source);

        if(useCombiner) {
            ICompletableFuture<List<AirportsMovementResult>> future = job
                    .mapper(new DestinationAirportMapper(specifiedOaci))
                    .combiner(new MovementCombinerFactory())
                    .reducer(new AirportsMovementReducerFactory())
                    .submit(new DestinationAirportCollator(quantity));
            result = future.get();
        }
        else {
            ICompletableFuture<List<AirportsMovementResult>> future = job
                    .mapper(new DestinationAirportMapper(specifiedOaci))
                    .reducer(new AirportsMovementReducerFactory())
                    .submit(new DestinationAirportCollator(quantity));
            result = future.get();

        }

        ResultWriter.writeResult4(resultPath, result);
    }

}

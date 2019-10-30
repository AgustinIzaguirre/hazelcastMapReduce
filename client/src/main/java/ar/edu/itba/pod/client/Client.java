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
//        queryNumber = 1; //TODO get from params
//        quantity = 5; //TODO remove on production
//        originOaci = "SAEZ"; //TODO remove on production
        loadProperties();
        ClientConfig config = loadClientConfig();
        final HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(config);
        final IMap<String, Airport> airportsMap = hazelcastInstance.getMap("g12-aeropuertos");
        final IMap<Long, Movement> movementsMap = hazelcastInstance.getMap("g12-movimientos");
        loadData(airportsMap, movementsMap);
        solveQuery(queryNumber, hazelcastInstance, airportsMap, movementsMap);
        timeFileWriter.close();
        System.out.println("Finished\n");//TODO remove
        hazelcastInstance.shutdown(); //TODO ask if has to shut down or not


        Map<Integer, List<Integer>> map = new HashMap<>();
        map.forEach((key,value)-> {
            System.out.println("hola");
        });
    }

    private static void loadProperties() throws RequiredPropertyException, InvalidQueryException {
        Optional<String> addressesProperty = Optional.ofNullable(System.getProperty("addresses"));
        Optional<String> inPathProperty = Optional.ofNullable(System.getProperty("inPath"));
        Optional<String> outPathProperty = Optional.ofNullable(System.getProperty("outPath"));
        queryNumber = Integer.parseInt(System.getProperty("queryNumber"));  // TODO use this on production
        addresses= addressesProperty.orElseThrow(() -> new RequiredPropertyException("addresses property is required.")).split(";");  // TODO use this on production check if null
        inputDirectoryPath = inPathProperty.orElseThrow(() -> new RequiredPropertyException("inPath property is required."));    // TODO use this on production
        outputDirectoryPath = outPathProperty.orElseThrow(() -> new RequiredPropertyException("outPath property is required."));  // TODO use this on production
        airportsFilePath = inputDirectoryPath + "/aeropuertos.csv";
        movementFilePath = inputDirectoryPath + "/movimientos.csv";
        resultFilePath = outputDirectoryPath + "/query" + queryNumber + ".csv"; // TODO use this on production
//        resultFilePath = outputDirectoryPath + "/query" + queryNumber + "_v2.csv";
        timeFilePath = outputDirectoryPath + "/query" + queryNumber + ".txt";

        if(queryNumber < 1 || queryNumber > 4) {
            throw new InvalidQueryException("Invalid query number.Query number should be 1,2,3 or 4.");// should never happen
        }

        if(queryNumber == 2 || queryNumber == 4) {
            Optional<String> quantityProperty = Optional.ofNullable(System.getProperty("n"));
            quantity = Long.parseLong(quantityProperty.orElseThrow(() -> new RequiredPropertyException("n property is required.")));

            if(queryNumber == 4) {
                Optional<String> oaciProperty = Optional.ofNullable(System.getProperty("originOaci"));
                originOaci = oaciProperty.orElseThrow(() -> new RequiredPropertyException("originOaci property is required."));
            }
        } //TODO use this on production
    }

    private static ClientConfig loadClientConfig() throws IOException {
        final ClientConfig config = new XmlClientConfigBuilder("hazelcast-client.xml").build();//TODO update with ips
        List<String> newAddresses = loadAddresses(); //TODO use on production
        config.getNetworkConfig().setAddresses(newAddresses); //TODO add in production
        return config;
    }

    private static List<String> loadAddresses() {
        List<String> newAddresses = new LinkedList<>();

        for(int i = 0; i < addresses.length; i++) { //TODO java 8 maybbe has for each for array
            newAddresses.add(addresses[i]);
        }

        return newAddresses;
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
        long startTime = System.currentTimeMillis(); //TODO remove
        long elapsedTime = 0; //TODO remove

        if(queryNumber == 1) {
            fileLoader.loadAirports(airportsFilePath, airportsMap);
        }

        fileLoader.loadMovements(movementFilePath, movementsMap);
        long endTime = System.currentTimeMillis(); //TODO remove
        elapsedTime = endTime - startTime; //TODO remove
        System.out.println("Loading time: " + elapsedTime); //TODO remove
        ResultWriter.writeTime(timeFileWriter, "Fin de lectura del archivo");
    }

    private static void solveQuery(int queryNumber, HazelcastInstance hazelcastInstance,
                                   IMap<String, Airport> airportsMap, IMap<Long, Movement> movementsMap)
                                    throws ExecutionException, InterruptedException, IOException {
        ResultWriter.writeTime(timeFileWriter, "Inicio del trabajo map/reduce");
//        boolean useCombiner = true;//TODO use lowest time combination
        long startTime = System.currentTimeMillis();//TODO remove
        long elapsedTime = 0;//TODO remove

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
                break;

            case 4:
                destinationAirports(hazelcastInstance, movementsMap, originOaci, quantity, true, resultFilePath);
                break;
        }

        ResultWriter.writeTime(timeFileWriter, "Fin del trabajo map/reduce");
        long endTime = System.currentTimeMillis(); //TODO remove
        elapsedTime = endTime - startTime; //TODO remove
        System.out.println("Processing time: " + elapsedTime); //TODO remove
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

    public static void pairAirportsWithSameThousandsWithSecondMapReduce(HazelcastInstance hazelcastInstance, IMap<Long,
                                                            Movement> movementsMap, boolean useCombiner, String resultPath)
                                                                throws ExecutionException, InterruptedException, IOException {
        //first MapReduce
        final KeyValueSource<Long, Movement> source = KeyValueSource.fromMap(movementsMap);
        JobTracker jobTracker = hazelcastInstance.getJobTracker("preCalculation");
        Map<String, Long> resultMap = null;
        Job<Long, Movement> job = jobTracker.newJob(source);

        if(useCombiner) {
            ICompletableFuture<Map<String, Long>> future = job
                    .mapper(new OaciAirportsMovementMapper())
                    .combiner(new MovementCombinerFactory())
                    .reducer(new AirportsMovementReducerFactory())
                    .submit();
            resultMap = future.get();
        }
        else {
            ICompletableFuture<Map<String, Long>> future = job
                    .mapper(new OaciAirportsMovementMapper()) //TODO use variable useCombiner
                    .reducer(new AirportsMovementReducerFactory())
                    .submit();
            resultMap = future.get();
        }

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
                                                        throws ExecutionException, InterruptedException, IOException {//TODO also add quantity
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

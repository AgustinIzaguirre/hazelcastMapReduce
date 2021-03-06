package ar.edu.itba.pod.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        logger.info("Server Starting ...");
        startServer("hazelcast.xml");
    }

    public static void startServer(String configPath) throws FileNotFoundException {
        Config config = new XmlConfigBuilder(configPath).build();
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
    }
}
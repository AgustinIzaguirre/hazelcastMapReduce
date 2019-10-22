package ar.edu.itba.pod.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        logger.info("Example Server Starting ...");
        Config config = new XmlConfigBuilder("hazelcast.xml").build();
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        ConcurrentMap<String, String> map = h.getMap("libros");
        map.put("key1", "hola como va");
        map.put("key2", "hola como");
        map.put("key3", "hola que tal");
        map.put("key4", "hola mundo");
    }
}
package ar.edu.itba.pod.client;

import ar.edu.itba.pod.models.Airport;
import ar.edu.itba.pod.results.AirportsMovementResult;
import com.hazelcast.core.IMap;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class ResultWriter {
    public static void writeResult1(String path, List<AirportsMovementResult> resultList,
                                    IMap<String, Airport> airportsMap) throws IOException {
        FileWriter result1File = new FileWriter(path);
        BufferedWriter result1Writer = new BufferedWriter(result1File);
        result1Writer.write("OACI;Denominación;Movimientos\n");
        resultList.forEach(element-> {
            Optional<Airport> airport = Optional.ofNullable(airportsMap.get(element.getOaciCode()));
            String denomination = airport.isPresent() ? airport.get().getName() : "";
            try {
                result1Writer.write(element.getOaciCode() + ";" + denomination + ";" + element.getMovements() + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        result1Writer.close();
    }
}

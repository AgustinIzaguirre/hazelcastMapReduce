package ar.edu.itba.pod.client;

import ar.edu.itba.pod.models.Airport;
import ar.edu.itba.pod.results.AirportPairResult;
import ar.edu.itba.pod.results.AirportsMovementResult;
import com.hazelcast.core.IMap;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class ResultWriter {
    public static void writeResult1(String outputFilePath, List<AirportsMovementResult> resultList,
                                    IMap<String, Airport> airportsMap) throws IOException {
        FileWriter result1File = new FileWriter(outputFilePath);
        BufferedWriter result1Writer = new BufferedWriter(result1File);
        result1Writer.write("OACI;Denominación;Movimientos\n");
        resultList.forEach(element-> {
            Optional<Airport> airport = Optional.ofNullable(airportsMap.get(element.getKey()));
            String denomination = airport.isPresent() ? airport.get().getName() : "";
            try {
                result1Writer.write(element.getKey() + ";" + denomination + ";" + element.getMovements() + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        result1Writer.close();
    }

    public static void writeResult2(String outputFilePath, List<AirportsMovementResult> resultList) throws IOException {
        FileWriter result2File = new FileWriter(outputFilePath);
        BufferedWriter result2Writer = new BufferedWriter(result2File);
        AtomicLong total = new AtomicLong();
        resultList.forEach(element -> total.addAndGet(element.getMovements()));
        result2Writer.write("Aerolinea;Porcentaje\n");
        resultList.forEach(element-> {
            try {
                double percentage = 100 * ((double)element.getMovements() / (double)total.get());
                String percentageFormated = new DecimalFormat("#.00").format(percentage) + "%";
                percentageFormated = percentageFormated.charAt(0) == '.' ? 0 + percentageFormated : percentageFormated;
                result2Writer.write(element.getKey() + ";" + percentageFormated + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        result2Writer.close();
    }

    public static void writeResult3(String outputFilePath, List<AirportPairResult> resultList) throws IOException {
        FileWriter result3File = new FileWriter(outputFilePath);
        BufferedWriter result3Writer = new BufferedWriter(result3File);
        result3Writer.write("Grupo;Aeropuerto A;Aeropuerto B\n");
        resultList.forEach(element-> {
            try {
                result3Writer.write(element.getMovements() + ";" + element.getFirst() + ";" + element.getSecond() + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        result3Writer.close();
    }

    public static void writeResult4(String outputFilePath, List<AirportsMovementResult> resultList) throws IOException {
        FileWriter result4File = new FileWriter(outputFilePath);
        BufferedWriter result4Writer = new BufferedWriter(result4File);
        result4Writer.write("OACI;Despegues\n");
        resultList.forEach(element-> {
            try {
                result4Writer.write(element.getKey() + ";" + element.getMovements() + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        result4Writer.close();
    }

    public static void writeTime(BufferedWriter outputFileWriter, String message) throws IOException {
        long time = new Date().getTime();
        Timestamp currentTime = new Timestamp(time);
        outputFileWriter.write(currentTime + "\tINFO Client -" + "\t" + message + "\n");
    }

}

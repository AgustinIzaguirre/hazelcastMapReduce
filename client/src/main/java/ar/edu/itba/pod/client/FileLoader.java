package ar.edu.itba.pod.client;

import ar.edu.itba.pod.models.Airport;
import ar.edu.itba.pod.models.Movement;
import com.hazelcast.core.IMap;

import java.io.*;
import java.util.Optional;

public class FileLoader {
    //airports indexes
    private int airportNameIndex;
    private int airportProvinceIndex;
    private int airportOaciCodeIndex;

    //movements indexes
    private int movementTypeIndex;
    private int flightClassIndex;
    private int originIndex;
    private int destinationIndex;
    private int airlineIndex;
    private int clasificationIndex;

    public void loadAirports(String path, IMap<String, Airport> map) throws IOException {
        FileReader airportsFile = new FileReader(path);
        BufferedReader airportReader = new BufferedReader(airportsFile);
        String line = airportReader.readLine();
        String airportData[] = line.split(";");
        loadAirportIndexes(airportData);

        do {
            line = airportReader.readLine();
            if(line != null) {
                airportData = line.split(";");
                Optional<Airport> currentAirport = loadAirport(airportData);
                currentAirport.ifPresent(airport -> map.put(airport.getOaciCode(), airport));
            }
        } while(line != null);

        airportReader.close();
    }

    private void loadAirportIndexes(String[] airportData) {
        for(int i = 0; i < airportData.length; i++) {
            String currentType = airportData[i].toUpperCase();

            if(currentType.equals("OACI")) {
                airportOaciCodeIndex = i;
            }
            else if(currentType.equals("DENOMINACION")) {
                airportNameIndex = i;
            }
            else if(currentType.equals("PROVINCIA")) {
                airportProvinceIndex = i;
            }
        }
    }

    private Optional<Airport> loadAirport(String[] airportData) {
        String name = null, province = null, oaciCode = null;

        if(airportData[airportOaciCodeIndex].length() > 0) {
            oaciCode = airportData[airportOaciCodeIndex];
        }
        else {
            return Optional.empty();
        }

        if(airportData[airportNameIndex].length() > 0) {
            name = airportData[airportNameIndex];
        }

        if(airportData[airportProvinceIndex].length() > 0) {
            province = airportData[airportProvinceIndex];
        }

        return Optional.of(new Airport(name, province, oaciCode));
    }

    public void loadMovements(String path, IMap<Long, Movement> map) throws IOException {
        FileReader movementsFile = new FileReader(path);
        BufferedReader movementReader = new BufferedReader(movementsFile);
        String line = movementReader.readLine();
        String movementData[] = line.split(";");
        loadMovementIndexes(movementData);

        do {
            line = movementReader.readLine();

            if(line != null) {
                movementData = line.split(";");
                Optional<Movement> currentMovementOptional = loadMovement(movementData);
                currentMovementOptional.ifPresent(currentMovement -> map.put(currentMovement.getId(), currentMovement));
            }
        } while(line != null);

        movementReader.close();
    }



    private void loadMovementIndexes(String[] movementData) {
        for(int i = 0; i < movementData.length; i++) {
            String currentType = movementData[i];

            if(currentType.equals("Origen OACI")) {
                originIndex = i;
            }
            else if(currentType.equals("Destino OACI")) {
                destinationIndex = i;
            }
            else if(currentType.equals("Aerolinea Nombre")) {
                airlineIndex = i;
            }
            else if(currentType.equals("Clase de Vuelo")) {
                flightClassIndex = i;
            }
            else if(currentType.length() > 11 && currentType.substring(0,10).equals("Clasificac")) {
                clasificationIndex = i;
            }
            else if(currentType.equals("Tipo de Movimiento")) {
                movementTypeIndex = i;
            }
        }
    }
    private Optional<Movement> loadMovement(String[] movementData) {
        String origin = null, destination = null, airline = null;
        String flightClass = null, clasification = null, type = null;

        if(movementData[originIndex].length() > 0) {
            origin = movementData[originIndex];
        }

        if(movementData[destinationIndex].length() > 0) {
            destination = movementData[destinationIndex];
        }

        if(movementData[airlineIndex].length() > 0) {
            airline = movementData[airlineIndex];
        }

        if(movementData[flightClassIndex].length() > 0) {
            flightClass = movementData[flightClassIndex];
        }

        if(movementData[clasificationIndex].length() > 0) {
            clasification = movementData[clasificationIndex];
        }

        if(movementData[movementTypeIndex].length() > 0) {
            type = movementData[movementTypeIndex];
        }

        if(origin == null || destination == null) {
            return Optional.empty();
        }

        return Optional.of(new Movement(type, flightClass, origin, destination, airline, clasification));
    }
}


package ar.edu.itba.pod.Util;

import java.io.*;

public class ResultComparator {
    public static boolean compareFiles(String filePath1, String filePath2) throws IOException {
        FileReader file1 = new FileReader(filePath1);
        FileReader file2 = new FileReader(filePath2);
        BufferedReader file1Reader = new BufferedReader(file1);
        BufferedReader file2Reader = new BufferedReader(file2);
        boolean finished = false, result = true;

        while(!finished) {
            String line1 = file1Reader.readLine();
            String line2 = file2Reader.readLine();

            if(line1 != null && line2 != null) {

                if(!line1.equals(line2)) {
                    finished = true;
                    result = false;
                }
            }
            else {
                finished = true;

                if((line1 != null && line2 == null) || (line2 != null && line1 == null)) {
                    result = false;
                }
            }
        }

        return result;
    }

    public static void main(String[] args) throws IOException {
        String path1 = "server/src/test/data/resultComparator/test.csv";
        String path2 = "server/src/test/data/resultComparator/test.csv";
        System.out.println(compareFiles(path1, path2));
    }
}

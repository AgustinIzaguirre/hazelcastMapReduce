package ar.edu.itba.pod;

import ar.edu.itba.pod.Util.ResultComparator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ResultComparatorTest {

    @Test
    public void twoEmptyFilesTest() throws IOException {
        //Set up
        System.out.println(new File("input.txt").getPath());
        String path1 = "src/test/data/resultComparator/emptyFile1.csv";
        String path2 = "src/test/data/resultComparator/emptyFile1.csv";


        //Action
        boolean result = ResultComparator.compareFiles(path1, path2);

        //Results
        Assert.assertEquals(true, result);
    }

    @Test
    public void diferentLengthFiles() throws IOException {
        //Set up
        String path1 = "src/test/data/resultComparator/longerFile.csv";
        String path2 = "src/test/data/resultComparator/shorterFile.csv";


        //Action
        boolean result1 = ResultComparator.compareFiles(path1, path2);
        boolean result2 = ResultComparator.compareFiles(path2, path1);

        //Results
        Assert.assertEquals(false, result1);
        Assert.assertEquals(false, result2);
    }

    @Test
    public void smaeLengthDifferentFiles() throws IOException {
        //Set up
        String path1 = "src/test/data/resultComparator/sameLengthFile1.csv";
        String path2 = "src/test/data/resultComparator/sameLengthFile2.csv";


        //Action
        boolean result1 = ResultComparator.compareFiles(path1, path2);
        boolean result2 = ResultComparator.compareFiles(path2, path1);

        //Results
        Assert.assertEquals(false, result1);
        Assert.assertEquals(false, result2);
    }

    @Test
    public void nonEmptyEqualFiles() throws IOException {
        //Set up
        String path1 = "src/test/data/resultComparator/equalFile1.csv";
        String path2 = "src/test/data/resultComparator/equalFile2.csv";


        //Action
        boolean result1 = ResultComparator.compareFiles(path1, path2);
        boolean result2 = ResultComparator.compareFiles(path2, path1);

        //Results
        Assert.assertEquals(true, result1);
        Assert.assertEquals(true, result2);
    }
}

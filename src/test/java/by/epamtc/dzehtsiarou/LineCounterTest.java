package by.epamtc.dzehtsiarou;

import org.apache.spark.sql.SparkSession;
import org.junit.*;

public class LineCounterTest {
    private static SparkSession spark;
    private static LineCounter counter;

    @BeforeClass
    public static void setUp() {
        counter = new LineCounter();
        spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Test")
                .getOrCreate();
    }

    @AfterClass
    public static void tearDown() {
        spark.stop();
    }

    @Test
    public void getLinesCount() {
        String pathToFile = "src/test/resources/test-text.txt";
        final long linesCount = counter.getLinesCount(spark, pathToFile);

        Assert.assertEquals(3, linesCount);
    }
}

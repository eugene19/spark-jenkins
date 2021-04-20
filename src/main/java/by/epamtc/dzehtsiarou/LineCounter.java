package by.epamtc.dzehtsiarou;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LineCounter {

    public 
        final Dataset<Row> text = spark.read().text(path);

        return text.count();
    }
}

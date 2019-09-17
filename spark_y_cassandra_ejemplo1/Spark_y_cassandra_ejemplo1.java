package spark_y_cassandra_ejemplo1;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapColumnTo;
import java.sql.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author TroyanoXD
 */
public class Spark_y_cassandra_ejemplo1 {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", "localhost")
                .set("spark.cassandra.connection.port","9042")
                .set("spark.cassandra.auth.username", "cassandra")
                .set("spark.cassandra.auth.password", "cassandra");

        conf.setAppName("Java API demo");
        conf.setMaster("spark://localhost:7077");
        conf.set("spark.testing.memory", "2147480000");

        JavaSparkContext sc = new JavaSparkContext(conf);
       
        JavaRDD<String> miId = javaFunctions(sc).cassandraTable("prueba", "person", mapColumnTo(String.class)).
                where("id = ?", "2019-02-12"); // Replace with the column you wish to select
// collect method fetches the records from&nbsp; RDD
        miId.collect().forEach((id) -> {
            System.out.println(id);
        });
System.out.println("ya termino");
    }

}

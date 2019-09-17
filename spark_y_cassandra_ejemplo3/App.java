package spark_y_cassandra_ejemplo3;


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.cql.CassandraConnector;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 *
 * @author TroyanoXD
 */
public class App 
{
    private final transient SparkConf conf;

    private App(SparkConf conf) {
        this.conf = conf;
    }
    private void run() {
        JavaSparkContext sc = new JavaSparkContext(conf);
        createSchema(sc);


        sc.stop();
    }

    private void createSchema(JavaSparkContext sc) {
        CassandraConnector connector = CassandraConnector.apply(sc.getConf());

        // Prepare the schema
        try (Session session = connector.openSession()) {
            session.execute("DROP KEYSPACE IF EXISTS tester");
            session.execute("CREATE KEYSPACE tester WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("CREATE TABLE tester.emp (id INT PRIMARY KEY, fname TEXT, lname TEXT, role TEXT)");
            session.execute("CREATE TABLE tester.dept (id INT PRIMARY KEY, dname TEXT)");       

            session.execute(
                      "INSERT INTO tester.emp (id, fname, lname, role) " +
                      "VALUES (" +
                          "0001," +
                          "'Angel'," +
                          "'Pay'," +
                          "'IT Engineer'" +
                          ");");
            session.execute(
                      "INSERT INTO tester.emp (id, fname, lname, role) " +
                      "VALUES (" +
                          "0002," +
                          "'John'," +
                          "'Doe'," +
                          "'IT Engineer'" +
                          ");");
            session.execute(
                      "INSERT INTO tester.emp (id, fname, lname, role) " +
                      "VALUES (" +
                          "0003," +
                          "'Jane'," +
                          "'Doe'," +
                          "'IT Analyst'" +
                          ");");
                session.execute(
                      "INSERT INTO tester.dept (id, dname) " +
                      "VALUES (" +
                          "1553," +
                          "'Commerce'" +
                          ");");

                ResultSet results = session.execute("SELECT * FROM tester.emp " +
                        "WHERE role = 'IT Engineer' allow filtering;");
            for (Row row : results) {
                System.out.print(row.getString("fname"));
                System.out.print(" ");
                System.out.print(row.getString("lname"));
                System.out.println(); 
            }
                System.out.println();
            }

        }

    public static void main( String[] args )
    {
         SparkConf conf = new SparkConf(true)
                .set("spark.cassandra.connection.host", "localhost")
                .set("spark.cassandra.connection.port","9042")
                .set("spark.cassandra.auth.username", "cassandra")
                .set("spark.cassandra.auth.password", "cassandra");

        conf.setAppName("Java APP");
        conf.setMaster("spark://localhost:7077");
        conf.set("spark.testing.memory", "2147480000");

              
        App app = new App(conf);
        app.run();
    }
}

package hbase;import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class Estudiantes {
    private static final String TABLE_NAME = "Estudiantes";
    private static final String COLUMN_FAMILY = "info";
    private static final String[] DATA = { "1,John,Doe", "2,Jane,Smith", "3,Michael,Johnson" };

    public static void main(String[] args) {
        // Create HBase configuration
        Configuration config = HBaseConfiguration.create();

        try {
            // Create connection to HBase
            Connection connection = ConnectionFactory.createConnection(config);

            // Create HBase admin
            Admin admin = connection.getAdmin();

            // Create table descriptor
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

            // Add column family
            HColumnDescriptor columnFamilyDescriptor = new HColumnDescriptor(COLUMN_FAMILY);
            tableDescriptor.addFamily(columnFamilyDescriptor);

            // Create the table
            System.out.println("Credando Tabla");
            if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
            admin.createTable(tableDescriptor);
            System.out.println("Tabla Creada");
            }

//            // Insert data into the table
//            for (String data : DATA) {
//                String[] parts = data.split(",");
//                String rowKey = parts[0];
//                String name = parts[1];
//                String surname = parts[2];
//
//                // TODO: Insert data into the table
//
//            }

            // Close the admin and connection
            admin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

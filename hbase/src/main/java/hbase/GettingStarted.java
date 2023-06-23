package hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class GettingStarted {
    static {
        // Comenta las siguientes líneas para desactivar temporalmente la configuración de log4j
        /*
        Logger.getRootLogger().setLevel(Level.OFF);
        Logger.getLogger("org.apache.hadoop.util.Shell").setLevel(Level.OFF);
        */
    }

    public static void main(String[] args) {
        try {
            // Configuración de HBase
            Configuration conf = HBaseConfiguration.create();

            // Establecemos las propiedades
            conf.set("hbase.rootdir", "file:///tmp/hbase/hbase");
            conf.set("hbase.zookeeper.property.dataDir", "//tmp/hbase/zookeeper");
            conf.set("hbase.unsafe.stream.capability.enforce", "false");
            System.out.println("Se ha conectado a HBase");

            // Conexión al cliente HBase
            Connection connection = ConnectionFactory.createConnection(conf);
            Table table = connection.getTable(TableName.valueOf("cliente"));
            System.out.println("Se ha conectado a la tabla 'cliente'");

            // Definir el escaneo
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);
            System.out.println("Comenzó a recorrer");

            // Recorrer los resultados
            for (Result res : scanner) {
                System.out.println("Dentro");
                System.out.println(res);
//                System.out.println(Bytes.toString(res.getRow()));
//                System.out.println(Bytes.toString(res.getValue("personal".getBytes(), "Nombre".getBytes())));
//                System.out.println(Bytes.toString(res.getValue("personal".getBytes(), "Apellido".getBytes())));
//                System.out.println(Bytes.toString(res.getValue("personal".getBytes(), "Telefono".getBytes())));
//                System.out.println(Bytes.toString(res.getValue("profesional".getBytes(), "Telefono".getBytes())));
            }

            // Cerrar recursos
            System.out.println("Recurso Cerrado");
            scanner.close();
            table.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

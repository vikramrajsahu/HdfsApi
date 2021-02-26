/**
 * HDFS JAVA API - Vikramraj
 */
import java.io.File;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class HdfsJavaApiOps {

    public static void copyFromLocal(String source, String dest, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        String filename = source.substring(source.lastIndexOf('/') + 1,source.length());
        if (dest.charAt(dest.length() - 1) != '/') {
            dest = dest + "/" + filename;
        } else {
            dest = dest + filename;
        }

        Path path = new Path(dest);
        if (fileSystem.exists(path)) {
            System.out.println("File " + dest + " already exists");
            return;
        }

        InputStream in = new BufferedInputStream(new FileInputStream(new File(source)));
        FSDataOutputStream out = fileSystem.create(path);

        byte[] b = new byte[in.available()];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        in.close();
        out.close();
        fileSystem.close();
    }

    public static void copyToLocal(String file, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        String filename = file.substring(file.lastIndexOf('/') + 1,file.length());

        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        FSDataInputStream in = fileSystem.open(path);
        OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(filename)));

        byte[] b = new byte[in.available()];
        int numBytes = 0;
        while ((numBytes = in.read(b)) > 0) {
            out.write(b, 0, numBytes);
        }

        in.close();
        out.close();
        fileSystem.close();
    }

    public static void rm(String file, Configuration conf) throws IOException {
        FileSystem fileSystem = FileSystem.get(conf);

        Path path = new Path(file);
        if (!fileSystem.exists(path)) {
            System.out.println("File " + file + " does not exists");
            return;
        }

        fileSystem.delete(new Path(file), true);
        fileSystem.close();
    }
}

public class HdfsJavaApi {

    static String CONFIG_NAME_DEFAULT_FS = "fs.defaultFS";
    static String FS_PREFIX = "hdfs://nameservice1";

    public static void main( String [] a) throws Exception {

        Configuration conf;

        String source = "", dest = "";
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        int choice;
        while(true) {
            // Show Options
            System.out.println("-------------------------------");
            System.out.println("Enter 1 for Local to HDFS");
            System.out.println("Enter 2 for HDFS to local");
            System.out.println("Enter 3 for deletion from HDFS");
            System.out.println("Enter 4 for exit...");
            System.out.println("-------------------------------");

            choice = Integer.parseInt(br.readLine());

            switch(choice) {
                case 1:
                    System.out.println("Enter local source and HDFS destination paths...");
                    source = br.readLine();
                    dest = br.readLine();
                    conf = new Configuration();
                    conf.set(CONFIG_NAME_DEFAULT_FS, FS_PREFIX);
                    HdfsJavaApiOps.copyFromLocal(source, dest, conf);
                    break;
                case 2:
                    System.out.println("Enter HDFS source...");
                    source = br.readLine();
                    conf = new Configuration();
                    conf.set(CONFIG_NAME_DEFAULT_FS, FS_PREFIX);
                    HdfsJavaApiOps.copyToLocal(source, conf);
                    break;
                case 3:
                    System.out.println("Enter HDFS source to be deleted...");
                    source=br.readLine();
                    conf = new Configuration();
                    conf.set(CONFIG_NAME_DEFAULT_FS, FS_PREFIX);
                    HdfsJavaApiOps.rm(source, conf);
                    break;
                case 4:
                    System.out.println("Exiting...");
                    return;
                default:
                    System.out.println("Error: Invalid Option selected. Please enter correct option");
                    break;
            }
        }
    }
}
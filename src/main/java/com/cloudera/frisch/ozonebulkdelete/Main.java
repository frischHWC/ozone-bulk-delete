package com.cloudera.frisch.ozonebulkdelete;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

@SuppressWarnings("unchecked")
@Slf4j
public class Main {

    public static void main(String [] args) {

        if (args.length < 5) {
            log.info("You need to provide 5 arguments in that order: keytab / kerberos user / Ozone service id / volume where to delete / number of days");
            System.exit(1);
        }

        String keytab = args[0];
        String kerberosUser = args[1];
        String ozoneServiceId = args[2];
        String volumeName = args[3];
        String numberOfDays = args[4];
        String dryRunArgs = args[5];
        Boolean dryRun = dryRunArgs!=null && dryRunArgs.equalsIgnoreCase("true") ;
        Instant nowMinusDays = Instant.now().minus(Long.parseLong(numberOfDays), ChronoUnit.DAYS);

        try {
            OzoneConfiguration config = new OzoneConfiguration();
            config.addResource(new Path("/etc/hadoop/conf.cloudera.hdfs/ozone-site.xml"));
            System.setProperty("HADOOP_USER_NAME", kerberosUser);
            System.setProperty("hadoop.home.dir", "/user/" + kerberosUser);

            loginUserWithKerberos(kerberosUser, keytab, config);

            // Setup ozone client to volume
            OzoneClient ozClient= OzoneClientFactory.getRpcClient(ozoneServiceId, config);
            ObjectStore objectStore = ozClient.getObjectStore();
            OzoneVolume volume = objectStore.getVolume(volumeName);

            // Deletion of keys
            volume.listBuckets(null).forEachRemaining(bucket -> {
                    log.info("Checking keys in bucket: " + bucket.getName() + " inside volume: " + volumeName);
                    try {
                        bucket.listKeys(null).forEachRemaining(key -> {
                            try {
                                if(key.getDataSize()!=0 && key.getModificationTime().isBefore(nowMinusDays)) {
                                    log.info("Key " + key.getName() + " as last modified time is : " + key.getModificationTime().toString());
                                    if(!dryRun) {
                                        bucket.deleteKey(key.getName());
                                    }
                                    log.info("Deleted key: " + key.getName() + " in bucket: " + bucket.getName() + " in volume: " + volumeName);
                                }
                            } catch (IOException e) {
                                log.error("cannot delete key : " + key.getName() + " in bucket: " + bucket.getName() + " in volume: " + volumeName + " due to error: ", e);
                            }
                        });
                    } catch (IOException e) {
                        log.error("Could not list keys in bucket " + bucket.getName() + " in volume: " + volumeName);
                    }
                });

            // Closing connection
            ozClient.close();
            logoutUserWithKerberos();

        } catch (IOException e) {
            log.error("Could execute deletion due to error: ", e);
        }

    }

    public static void loginUserWithKerberos(String kerberosUser, String pathToKeytab, Configuration config) {
        if(config != null) {
            config.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(config);
        }
        try {
            UserGroupInformation.loginUserFromKeytab(kerberosUser, pathToKeytab);
        } catch (IOException e) {
            log.error("Could not load keytab file",e);
        }
    }

    public static void logoutUserWithKerberos() {
        try {
            UserGroupInformation.getCurrentUser().logoutUserFromKeytab();
            UserGroupInformation.getLoginUser().logoutUserFromKeytab();
            UserGroupInformation.reset();
        } catch (Exception e) {
            log.warn("Could not logout user from kerberos",e);
        }
    }



}

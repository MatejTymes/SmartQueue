package mtymes.test.db;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.Feature;
import de.flapdoodle.embed.mongo.distribution.IFeatureAwareVersion;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.extract.UserTempNaming;
import de.flapdoodle.embed.process.io.directories.FixedPath;
import org.bson.Document;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.EnumSet;

import static de.flapdoodle.embed.process.runtime.Network.getFreeServerPort;
import static de.flapdoodle.embed.process.runtime.Network.localhostIsIPv6;
import static mtymes.common.mongo.DocBuilder.docBuilder;
import static mtymes.common.mongo.DocBuilder.emptyDoc;

// doc: https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo
public class EmbeddedDB {

    public enum CustomVersion implements IFeatureAwareVersion {

        //        V3_6_10("3.6.10", Feature.SYNC_DELAY, Feature.STORAGE_ENGINE, Feature.ONLY_64BIT, Feature.NO_CHUNKSIZE_ARG, Feature.MONGOS_CONFIGDB_SET_STYLE, Feature.NO_HTTP_INTERFACE_ARG, Feature.ONLY_WITH_SSL, Feature.ONLY_WINDOWS_2008_SERVER, Feature.NO_SOLARIS_SUPPORT, Feature.NO_BIND_IP_TO_LOCALHOST),
        V4_0_12("4.0.12", Feature.SYNC_DELAY, Feature.STORAGE_ENGINE, Feature.ONLY_64BIT, Feature.NO_CHUNKSIZE_ARG, Feature.MONGOS_CONFIGDB_SET_STYLE, Feature.NO_HTTP_INTERFACE_ARG, Feature.ONLY_WITH_SSL, Feature.ONLY_WINDOWS_2008_SERVER, Feature.NO_SOLARIS_SUPPORT, Feature.NO_BIND_IP_TO_LOCALHOST),
        V4_2_21("4.2.21", Feature.SYNC_DELAY, Feature.STORAGE_ENGINE, Feature.ONLY_64BIT, Feature.NO_CHUNKSIZE_ARG, Feature.MONGOS_CONFIGDB_SET_STYLE, Feature.NO_HTTP_INTERFACE_ARG, Feature.ONLY_WITH_SSL, Feature.ONLY_WINDOWS_2008_SERVER, Feature.NO_SOLARIS_SUPPORT, Feature.NO_BIND_IP_TO_LOCALHOST);

        private final String specificVersion;
        private final EnumSet<Feature> features;

        CustomVersion(String specificVersion, Feature... features) {
            this.specificVersion = specificVersion;
            this.features = Feature.asSet(features);
        }


        @Override
        public boolean enabled(Feature feature) {
            return features.contains(feature);
        }

        @Override
        public EnumSet<Feature> getFeatures() {
            return features;
        }

        @Override
        public String asInDownloadPath() {
            return specificVersion;
        }
    }


    //    private static final IFeatureAwareVersion USED_VERSION = Version.V3_4_15;
    private static final IFeatureAwareVersion USED_VERSION = CustomVersion.V4_0_12;
//    private static final IFeatureAwareVersion USED_VERSION = CustomVersion.V4_2_21;

    private final int port;
    private final String dbName;

    private MongodExecutable executable;
    private MongodProcess process;
    private MongoDatabase database;
    private boolean started = false;

    public EmbeddedDB(int port, String dbName) {
        this.port = port;
        this.dbName = dbName;
    }

    public static EmbeddedDB embeddedDB() {
        try {
            int port = getFreeServerPort();

            return new EmbeddedDB(port, "myBank");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getDbName() {
        return dbName;
    }

    public int getPort() {
        return port;
    }

    public synchronized MongoDatabase getDatabase() {
        if (!started) {
            throw new IllegalStateException("Embedded MongoDB not started yet started");
        }
        return database;
    }

    public void removeAllData() {
        for (String collectionName : database.listCollectionNames()) {
            database.getCollection(collectionName).deleteMany(emptyDoc());
        }
    }

    public synchronized EmbeddedDB start() {
        if (started) {
            throw new IllegalStateException("Embedded MongoDB already started");
        }
        try {
            IMongodConfig config = new MongodConfigBuilder()
                    .version(USED_VERSION)
                    .net(new Net("localhost", port, localhostIsIPv6()))
//                    .setParameter("ttlMonitorSleepSecs", "5")
//                    .setParameter("ttlMonitorSleepSecs", "0")
                    .build();
            Command command = Command.MongoD;
            IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                    .defaults(command)
                    .artifactStore(new ExtractedArtifactStoreBuilder()
                            .defaults(command)
                            .download(new DownloadConfigBuilder()
                                    .defaultsForCommand(command)
                                    .artifactStorePath(new FixedPath("build/mongo"))
//                                    .packageResolver(new Paths(command) {
//                                        @Override
//                                        public String getPath(Distribution distribution) {
//                                            return super.getPath(distribution);
//                                        }
//                                    })
//                                    .downloadPath("https://fastdl.mongodb.org/win32/") // mongodb-win32-x86_64-2012plus-4.2.21.zip
                                    .build())
                            .executableNaming(new UserTempNaming() {
                                @Override
                                public String nameFor(String prefix, String postfix) {
                                    String execName = super.nameFor(prefix, postfix);

                                    try {
                                        String tempFolder = System.getenv("temp") + File.separator;
                                        File executableFile = new File(tempFolder + execName);
                                        Files.deleteIfExists(executableFile.toPath());
                                        File pidFile = execName.contains(".")
                                                ? new File(tempFolder + execName.substring(0, execName.lastIndexOf(".")) + ".pid")
                                                : new File(tempFolder + execName + ".pid");
                                        Files.deleteIfExists(pidFile.toPath());
                                    } catch (IOException e) {
                                        throw new IllegalStateException(e);
                                    }

                                    return execName;
                                }
                            }))
                    .build();

            MongodStarter starter = MongodStarter.getInstance(runtimeConfig);
            executable = starter.prepare(config);
            process = executable.start();
            started = true;

            MongoClient client = new MongoClient("localhost", this.port);

            // this doesn't seem to work
            MongoDatabase adminDb = client.getDatabase("admin");
//            Document response = adminDb.runCommand(
//                    docBuilder()
//                            .put("setParameter", 1)
//                            .put("ttlMonitorEnabled", true)
//                            .build()
//            );
            Document response = adminDb.runCommand(
                    docBuilder()
                            .put("setParameter", 1)
//                            .put("ttlMonitorSleepSecs", 5)
                            .put("ttlMonitorSleepSecs", 0)
                            .build()
            );
//            Document response = adminDb.runCommand(
//                    docBuilder()
//                            .put("setParameter", 1)
//                            .put("ttlMonitorEnabled", true)
//                            .build()
//            );

            database = client.getDatabase(dbName);

            return this;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void stop() {
        if (!started) {
            throw new IllegalStateException("Embedded MongoDB not started yet started");
        }
        process.stop();
        executable.stop();
    }

    public static void main(String[] args) throws IOException {
        EmbeddedDB embeddedDB = embeddedDB().start();

        embeddedDB.stop();
    }
}

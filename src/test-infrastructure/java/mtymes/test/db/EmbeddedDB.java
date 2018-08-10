package mtymes.test.db;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import de.flapdoodle.embed.mongo.Command;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.IFeatureAwareVersion;
import de.flapdoodle.embed.mongo.distribution.Versions;
import de.flapdoodle.embed.process.config.IRuntimeConfig;
import de.flapdoodle.embed.process.distribution.GenericVersion;
import de.flapdoodle.embed.process.extract.UserTempNaming;
import de.flapdoodle.embed.process.io.directories.FixedPath;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static de.flapdoodle.embed.mongo.distribution.Feature.*;
import static de.flapdoodle.embed.process.runtime.Network.getFreeServerPort;
import static de.flapdoodle.embed.process.runtime.Network.localhostIsIPv6;
import static mtymes.common.mongo.DocBuilder.emptyDoc;

// doc: https://github.com/flapdoodle-oss/de.flapdoodle.embed.mongo
public class EmbeddedDB {

    private static final IFeatureAwareVersion V3_4_9 = Versions.withFeatures(new GenericVersion("3.4.9"), SYNC_DELAY, STORAGE_ENGINE, ONLY_64BIT, NO_CHUNKSIZE_ARG, MONGOS_CONFIGDB_SET_STYLE);

    // todo: mtymes - make work with newer versions of mongodb
    private static final IFeatureAwareVersion USED_VERSION = V3_4_9;
//    private static final IFeatureAwareVersion USED_VERSION = Version.V3_6_5;

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
                    .build();
            Command command = Command.MongoD;
            IRuntimeConfig runtimeConfig = new RuntimeConfigBuilder()
                    .defaults(command)
                    .artifactStore(new ExtractedArtifactStoreBuilder()
                            .defaults(command)
                            .download(new DownloadConfigBuilder()
                                    .defaultsForCommand(command)
                                    .artifactStorePath(new FixedPath("build/mongo"))
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

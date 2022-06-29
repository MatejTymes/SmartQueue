package mtymes.smartqueue.dao.mongo;

import com.mongodb.client.MongoDatabase;
import mtymes.smartqueue.dao.BaseTaskTest;
import mtymes.smartqueue.taskHandler.TaskHandler;
import mtymes.smartqueue.taskHandler.mongo.MongoTaskHandler;
import mtymes.test.db.EmbeddedDB;
import mtymes.test.db.MongoManager;
import mtymes.test.time.FixedClock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.time.ZonedDateTime;
import java.util.Optional;

import static mtymes.common.time.DateUtil.UTC_ZONE_ID;
import static mtymes.smartqueue.dao.mongo.MongoCollections.bodiesCollection;
import static mtymes.smartqueue.dao.mongo.MongoCollections.tasksCollection;

public class MongoTaskDaoIntegrationTest extends BaseTaskTest {

    private static final FixedClock clock = new FixedClock();

    private static EmbeddedDB db;
    private static MongoTaskHandler mongoTaskHandler;

    @BeforeClass
    public static void initDB() {
        db = MongoManager.getEmbeddedDB();

        MongoDatabase database = db.getDatabase();
        MongoTaskDao taskDao = new MongoTaskDao(
                tasksCollection(database, "tasks"),
                Optional.of(bodiesCollection(database, "bodies")),
                clock
        );

        mongoTaskHandler = new MongoTaskHandler(
                taskDao,
                clock
        );
    }

    @Before
    public void setUp() {
        db.removeAllData();

        mongoTaskHandler.clearData();

        clock.setNow(ZonedDateTime.now(UTC_ZONE_ID));
    }

    @AfterClass
    public static void releaseDB() {
        MongoManager.release(db);
    }

    @Override
    protected TaskHandler taskHandler() {
        return mongoTaskHandler;
    }
}

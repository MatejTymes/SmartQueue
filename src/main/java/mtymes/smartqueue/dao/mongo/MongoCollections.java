package mtymes.smartqueue.dao.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;

import java.util.function.Consumer;

import static com.mongodb.client.model.Indexes.ascending;
import static javafixes.common.CollectionUtil.newSet;
import static mtymes.common.mongo.DocBuilder.doc;
import static mtymes.common.mongo.DocBuilder.docBuilder;

public class MongoCollections {

    public static MongoCollection<Document> tasksCollection(MongoDatabase database) {
        return getOrCreateCollection(
                database,
                "tasks",
                tasks -> {
                    tasks.createIndex(
                            ascending(
                                    MongoTaskDao.LAST_EXECUTION_ID
                            ),
                            new IndexOptions().unique(false)
                    );
                    tasks.createIndex(
                            ascending(
                                    // todo: make dynamic based on sorting in next createNextExecution(...)
                                    MongoTaskDao.UPDATED_AT_TIME
                            ),
                            new IndexOptions()
                                    .partialFilterExpression(
                                            docBuilder()
                                                    .put(MongoTaskDao.IS_AVAILABLE_FOR_EXECUTION, true)
                                                    .put(MongoTaskDao.EXECUTION_ATTEMPTS_LEFT, doc("$gt", 0))
                                                    .build()
                                    ).unique(false)
                    );
                }
        );
    }

    public static MongoCollection<Document> bodiesCollection(MongoDatabase database) {
        return getOrCreateCollection(
                database,
                "bodies",
                bodies -> {
                    // no indexes needed
                }
        );
    }

    private static MongoCollection<Document> getOrCreateCollection(MongoDatabase database, String collectionName, Consumer<MongoCollection<Document>> afterCreation) {
        if (!newSet(database.listCollectionNames()).contains(collectionName)) {
            database.createCollection(collectionName);

            MongoCollection<Document> collection = database.getCollection(collectionName);
            afterCreation.accept(collection);
            return collection;
        } else {
            return database.getCollection(collectionName);
        }
    }
}

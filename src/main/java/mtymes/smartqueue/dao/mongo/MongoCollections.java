package mtymes.smartqueue.dao.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;

import java.util.function.Consumer;

import static com.mongodb.client.model.Indexes.ascending;
import static com.mongodb.client.model.Indexes.descending;
import static javafixes.common.CollectionUtil.newSet;

public class MongoCollections {

    public static MongoCollection<Document> jobRequestsCollection(MongoDatabase database) {
        return getOrCreateCollection(
                database,
                "jobRequests",
                jobs -> {
                    jobs.createIndex(
                            ascending(
                                    MongoJobDao.STATE
//                                    ,
//                                    MongoJobDao.UPDATED_AT_TIME
                            ),
                            new IndexOptions().unique(false)
                    );
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

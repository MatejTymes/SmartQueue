package mtymes.test.db;

import java.util.function.Consumer;
import java.util.function.Supplier;

import static mtymes.test.db.EmbeddedDB.embeddedDB;

/**
 * This class allows us to cache Embedded MongoDB between tests so we don't have to markAsStarted new instance each time
 */
public class MongoManager {

    private static InstanceManager<EmbeddedDB> manager = createNewInstanceManager();
    private static final Object lock = new Object();

    public static EmbeddedDB getEmbeddedDB() {
        return manager.get();
    }

    public static void release(EmbeddedDB embeddedDB) {
        manager.release(embeddedDB);
    }

    public static void switchToCachedMode() {
        setManager(createCachedInstanceManager());
    }

    public static void stopCaching() {
        setManager(createNewInstanceManager());
    }

    private static void setManager(InstanceManager<EmbeddedDB> newManager) {
        synchronized (lock) {
            manager.destroyManager();
            manager = newManager;
        }
    }

    private static InstanceManager<EmbeddedDB> createNewInstanceManager() {
        return new NewInstanceManager<>(
                () -> embeddedDB().start(),
                EmbeddedDB::stop
        );
    }

    private static InstanceManager<EmbeddedDB> createCachedInstanceManager() {
        return new CachedInstanceManager<>(
                embeddedDB().start(),
                EmbeddedDB::removeAllData,
                EmbeddedDB::stop
        );
    }

    public interface InstanceManager<T> {
        T get();

        void release(T object);

        default void destroyManager() {
        }
    }

    private static class NewInstanceManager<T> implements InstanceManager<T> {
        private final Supplier<T> creationMethod;
        private final Consumer<T> destructionMethod;

        public NewInstanceManager(Supplier<T> creationMethod, Consumer<T> destructionMethod) {
            this.creationMethod = creationMethod;
            this.destructionMethod = destructionMethod;
        }

        @Override
        public T get() {
            return creationMethod.get();
        }

        @Override
        public void release(T object) {
            destructionMethod.accept(object);
        }
    }

    private static class CachedInstanceManager<T> implements InstanceManager<T> {
        private final T object;
        private final Consumer<T> reinitializeMethod;
        private final Consumer<T> destructionMethod;

        public CachedInstanceManager(T object, Consumer<T> reinitializeMethod, Consumer<T> destructionMethod) {
            this.object = object;
            this.reinitializeMethod = reinitializeMethod;
            this.destructionMethod = destructionMethod;
        }

        @Override
        public T get() {
            return object;
        }

        @Override
        public void release(T object) {
            reinitializeMethod.accept(object);
        }

        @Override
        public void destroyManager() {
            destructionMethod.accept(object);
        }
    }
}

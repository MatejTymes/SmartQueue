package mtymes;

import mtymes.test.db.MongoManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.extensions.dynamicsuite.Directory;
import org.junit.extensions.dynamicsuite.Filter;
import org.junit.extensions.dynamicsuite.TestClassFilter;
import org.junit.extensions.dynamicsuite.suite.DynamicSuite;
import org.junit.runner.RunWith;

import java.lang.reflect.Modifier;

@RunWith(DynamicSuite.class)
@Filter(IntegrationTestSuite.class)
@Directory("src/test-integration/java")
public class IntegrationTestSuite implements TestClassFilter {

    @BeforeClass
    public static void setUpSuite() throws Exception {
        MongoManager.switchToCachedMode();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        MongoManager.stopCaching();
    }

    @Override
    public boolean include(String className) {
        return className.endsWith("Test");
    }

    @Override
    public boolean include(Class cls) {
        return !Modifier.isAbstract(cls.getModifiers());
    }
}

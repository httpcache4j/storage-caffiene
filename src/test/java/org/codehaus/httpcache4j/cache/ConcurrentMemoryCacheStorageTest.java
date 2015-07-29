package org.codehaus.httpcache4j.cache;

/**
 * Created by maedhros on 29/07/15.
 */
public class ConcurrentMemoryCacheStorageTest extends CacheStorageAbstractTest {
    @Override
    protected CacheStorage createCacheStorage() {
        return new ConcurrentMemoryCacheStorage();
    }

    @Override
    protected void afterTest() {

    }
}

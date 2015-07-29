package org.codehaus.httpcache4j.cache;

import com.github.benmanes.caffeine.cache.*;
import org.codehaus.httpcache4j.HTTPRequest;
import org.codehaus.httpcache4j.HTTPResponse;
import org.codehaus.httpcache4j.payload.ByteArrayPayload;
import org.codehaus.httpcache4j.payload.Payload;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

public final class ConcurrentMemoryCacheStorage implements CacheStorage {
    private final Cache<URI, Map<Vary, CacheItem>> cache;
    private final ExecutorService executor;

    public ConcurrentMemoryCacheStorage() { this(1000, 1); }

    public ConcurrentMemoryCacheStorage(long maxSize, int maxThreads) {
        executor = Executors.newFixedThreadPool(maxThreads);
        cache = Caffeine.newBuilder().
                maximumSize(maxSize).
                name(() -> getClass().getSimpleName()).
                initialCapacity(100).
                executor(executor).
                build();
    }

    public HTTPResponse insert(HTTPRequest request, HTTPResponse response) {
        Key key = Key.create(request, response);
        HTTPResponse cacheableResponse = rewriteResponse(response);
        invalidate(key);
        return putImpl(key, cacheableResponse);
    }

    public HTTPResponse update(HTTPRequest request, HTTPResponse response) {
        Key key = Key.create(request, response);
        return putImpl(key, response);
    }

    public CacheItem get(Key key) {
        Map<Vary, CacheItem> map = cache.getIfPresent(key.getURI());
        return map != null ? map.get(key.getVary()) : null;
    }

    public CacheItem get(HTTPRequest request) {
        Map<Vary, CacheItem> varyCacheItemMap = cache.getIfPresent(request.getNormalizedURI());
        if (varyCacheItemMap == null) {
            return null;
        }
        else {
            for (Map.Entry<Vary, CacheItem> entry : varyCacheItemMap.entrySet()) {
                if (entry.getKey().matches(request)) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    public void invalidate(URI uri) {
        cache.invalidate(uri);
    }

    public void clear() {
        cache.invalidateAll();
    }

    public int size() {
        return (int) cache.estimatedSize();
    }

    public void shutdown() {
        clear();
        executor.shutdown();
    }

    public Iterator<Key> iterator() {
        Stream<Map.Entry<URI, Map<Vary, CacheItem>>> s = cache.asMap().entrySet().stream();
        Stream<Key> keyStream = s.flatMap(e -> e.getValue().keySet().stream().map(v -> new Key(e.getKey(), v)));
        return keyStream.iterator();
    }

    private HTTPResponse rewriteResponse(HTTPResponse response) {
        if (response.hasPayload()) {
            Payload payload = response.getPayload().get();
            try(InputStream stream = payload.getInputStream()) {
                return response.withPayload(createPayload(payload, stream));
            } catch (IOException ignore) {
            }
        }
        else {
            return response;
        }
        throw new IllegalArgumentException("Unable to cache response");
    }

    private HTTPResponse putImpl(final Key key, final HTTPResponse response) {
        CacheItem item = new DefaultCacheItem(response);
        Map<Vary, CacheItem> varyCacheItemMap = cache.getIfPresent(key.getURI());
        if (varyCacheItemMap == null) {
            varyCacheItemMap = new ConcurrentHashMap<>(5);
            cache.put(key.getURI(), varyCacheItemMap);
        }
        varyCacheItemMap.put(key.getVary(), item);
        return response;
    }

    private Payload createPayload(Payload payload, InputStream stream) throws IOException {
        ByteArrayPayload p = new ByteArrayPayload(stream, payload.getMimeType());
        if (p.isAvailable()) {
            return p;
        }
        return null;
    }

    private void invalidate(Key key) {
        Map<Vary, CacheItem> varyCacheItemMap = cache.getIfPresent(key.getURI());
        if (varyCacheItemMap != null) {
            varyCacheItemMap.remove(key.getVary());
            if (varyCacheItemMap.isEmpty()) {
                cache.invalidate(key.getURI());
            }
        }
    }
}

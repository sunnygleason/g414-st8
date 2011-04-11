package ptest.com.g414.st8.count;

import java.util.Map;

import com.sun.faban.driver.HttpClientFacade;
import com.sun.faban.driver.HttpRequestMethod;

public class CounterServiceOperations {
    private static final String BASE_URL = "/1.0/c";

    public static void create(String host, int port, HttpClientFacade f,
            String key, Map<String, Object> value) throws Exception {
        f.asReadRequest()
                .withUrl("http://" + host + ":" + port + BASE_URL + "/" + key)
                .withRequestMethod(HttpRequestMethod.POST).withPostRequest("")
                .execute();
    }

    public static void retrieve(String host, int port, HttpClientFacade f,
            String key) throws Exception {
        f.asReadRequest()
                .withUrl("http://" + host + ":" + port + BASE_URL + "/" + key)
                .withRequestMethod(HttpRequestMethod.GET).execute();
    }

    public static void delete(String host, int port, HttpClientFacade f,
            String key) throws Exception {
        f.asReadRequest()
                .withUrl("http://" + host + ":" + port + BASE_URL + "/" + key)
                .withRequestMethod(HttpRequestMethod.DELETE).execute();
    }
}
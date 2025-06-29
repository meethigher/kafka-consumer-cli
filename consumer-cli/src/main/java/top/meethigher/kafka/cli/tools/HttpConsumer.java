package top.meethigher.kafka.cli.tools;

import com.alibaba.fastjson2.JSON;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.meethigher.kafka.Record;

import javax.net.ssl.*;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

/**
 * 发送与接收方的服务规定
 *
 *
 * <pre>{@code curl -L "http://127.0.0.1:8080/kafka/records" \
 * -H "Content-Type: application/json" \
 * -d "[
 *     {
 *         \"topic\": \"\",
 *         \"partition\": 1,
 *         \"offset\": 1,
 *         \"key\": \"key\",
 *         \"value\": \"value\",
 *         \"timestamp\": 1111,
 *         \"timestampType\": \"createTime\"
 *     }
 * ]" }</pre>
 */
public class HttpConsumer extends AutoFlushList<Record> {

    private static final Logger log = LoggerFactory.getLogger(HttpConsumer.class);

    private final String url;

    public HttpConsumer(int maxSize, long timeoutMillis, String url) {
        super(maxSize, timeoutMillis);
        this.url = url;
    }

    @Override
    protected void onFlush(ConcurrentLinkedQueue<Record> batch) {
        try {
            int size = batch.size();
            RequestBody body = RequestBody.create(MediaType.parse("application/json"), JSON.toJSONString(batch));
            Request request = new Request.Builder()
                    .url(url)
                    .post(body)
                    .build();
            try (Response response = okHttpClient().newCall(request).execute()) {
                if (response.isSuccessful()) {
                    log.info("http onFlush successful, size {}", size);
                }
            }
        } catch (Exception e) {
            log.error("http onFlush error", e);
        }
    }


    private static OkHttpClient okHttpClient;


    public static Dispatcher getDispatcher() {
        Dispatcher dispatcher = new Dispatcher();
        dispatcher.setMaxRequests(2000);
        dispatcher.setMaxRequestsPerHost(2000);
        return dispatcher;
    }


    public synchronized static OkHttpClient okHttpClient() {
        if (okHttpClient == null) {
            Dispatcher dispatcher = getDispatcher();
            OkHttpClient.Builder builder = new OkHttpClient().newBuilder().retryOnConnectionFailure(false).dispatcher(dispatcher).connectionPool(new ConnectionPool(2, 60, TimeUnit.SECONDS)).connectTimeout(10, TimeUnit.SECONDS).readTimeout(60, TimeUnit.SECONDS).writeTimeout(60, TimeUnit.SECONDS);
            builder.followRedirects(false);
            builder.followSslRedirects(false);
            builder.hostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(final String s, final SSLSession sslSession) {
                    return true;
                }

                @Override
                public final String toString() {
                    return "NO_OP";
                }
            });
            try {
                X509TrustManager x509TrustManager = new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                };
                TrustManager[] trustManagers = {x509TrustManager};
                SSLContext sslContext = SSLContext.getInstance("SSL");
                sslContext.init(null, trustManagers, new SecureRandom());
                builder.sslSocketFactory(sslContext.getSocketFactory(), x509TrustManager);
            } catch (Exception ignore) {

            }
            okHttpClient = builder.build();
        }
        return okHttpClient;
    }

}

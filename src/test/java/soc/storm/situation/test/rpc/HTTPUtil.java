
package soc.storm.situation.test.rpc;

import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.conn.ClientConnectionManager;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

/**
 * 
 * @author wangbin03
 *
 */
public class HTTPUtil {
    private static PoolingClientConnectionManager connectionManager;
    private static CookieStore cookieStore;
    private static HttpContext httpContext;

    /**
     * 
     * @return
     * @throws Exception
     */
    private static ClientConnectionManager getConnectionManager() throws Exception {
        if (connectionManager == null) {
            synchronized (HTTPUtil.class) {
                if (connectionManager == null) {
                    SchemeRegistry schemeRegistry = new SchemeRegistry();
                    schemeRegistry.register(new Scheme("http", 80, PlainSocketFactory.getSocketFactory()));
                    SSLContext sslcontext = SSLContext.getInstance("SSL");
                    sslcontext.init(null, new TrustManager[] { new TrustAnyTrustManager() }, new SecureRandom());
                    SSLSocketFactory socketFactory = new SSLSocketFactory(sslcontext, SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
                    schemeRegistry.register(new Scheme("https", 443, socketFactory));
                    connectionManager = new PoolingClientConnectionManager(schemeRegistry);
                    connectionManager.setMaxTotal(2000);
                    connectionManager.setDefaultMaxPerRoute(800);
                }
            }
        }
        return connectionManager;
    }

    /**
     * 
     * @return
     * @throws Exception
     */
    public static HttpClient getClient() throws Exception {
        HttpClient httpClient = new DefaultHttpClient(getConnectionManager());
        httpClient.getParams().setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 10000);
        httpClient.getParams().setParameter(CoreConnectionPNames.SO_TIMEOUT, 360000);
        if (httpContext == null) {
            cookieStore = new BasicCookieStore();
            httpContext = new BasicHttpContext();
            httpContext.setAttribute(ClientContext.COOKIE_STORE, cookieStore);
        }
        return httpClient;
    }

    /**
     * 
     * @author wangbin03
     *
     */
    private static class TrustAnyTrustManager implements X509TrustManager {
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[] {};
        }
    }

}

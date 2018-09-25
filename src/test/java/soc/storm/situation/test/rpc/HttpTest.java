
package soc.storm.situation.test.rpc;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.cookie.Cookie;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import com.google.gson.JsonObject;

/**
 * 
 * @author wangbin03
 *
 */
public class HttpTest {

    private Log logger = LogFactory.getLog(this.getClass());

    private static String apiURL;// 接口地址
    private DefaultHttpClient httpClient = null;
    private HttpPost method = null;
    private long startTime = 0L;// 开始时间
    private long endTime = 0L;// 结束时间
    private int status = 0;// 0.成功 1.执行方法失败 2.协议错误 3.网络错误

    //
    private static CookieStore cookieStore;

    /**
     * 接口地址
     * 
     * @param url
     */
    public HttpTest(String url) {
        if (url != null) {
            HttpTest.apiURL = url;
        }

        if (apiURL != null) {
            // httpClient = new DefaultHttpClient();
            try {
                httpClient = (DefaultHttpClient) HTTPUtil.getClient();
            } catch (Exception e) {
                e.printStackTrace();
            }
            method = new HttpPost(apiURL);
        }
    }

    /**
     * 用户登录
     * 
     * @param parameters
     * @return
     */
    public String loginMethod(String parameters) {
        String body = null;
        logger.info("parameters:" + parameters);

        if (method != null & parameters != null && !"".equals(parameters.trim())) {
            try {
                // 建立一个NameValuePair数组，用于存储欲传送的参数
                method.addHeader("Content-type", "application/json; charset=utf-8");
                method.setHeader("Accept", "application/json");
                method.setEntity(new StringEntity(parameters, Charset.forName("UTF-8")));
                startTime = System.currentTimeMillis();

                HttpResponse response = httpClient.execute(method);

                endTime = System.currentTimeMillis();
                int statusCode = response.getStatusLine().getStatusCode();

                logger.info("statusCode:" + statusCode);
                logger.info("调用API 花费时间(单位：毫秒)：" + (endTime - startTime));
                if (statusCode != HttpStatus.SC_OK) {
                    logger.error("Method failed:" + response.getStatusLine());
                    status = 1;
                }

                //
                cookieStore = httpClient.getCookieStore();
                //
                // NEW_VISTOR true
                // dbName_bp test
                // BasicClientCookie bcookie = new BasicClientCookie("NEW_VISTOR", cookie.getValue());
                // bcookie.setDomain(cookie.getDomain());
                // bcookie.setExpiryDate(cookie.getExpiry());
                // bcookie.setPath(cookie.getPath());
                // cookiestore.addCookie(bcookie);
                // cookieStore.addCookie();
                //
                List<Cookie> cookieList = cookieStore.getCookies();
                System.out.println("cookieList.size():" + cookieList.size());
                // 得到Cookie
                StringBuffer tmpcookies = new StringBuffer();
                for (Iterator<Cookie> iterator = cookieList.iterator(); iterator.hasNext();) {
                    Cookie cookie = (Cookie) iterator.next();
                    tmpcookies.append(cookie.toString() + ";");
                    System.out.println("cookies = " + cookie.toString());
                    System.out.println("cookie.getName():" + cookie.getName() + ",cookie.getValue():" + cookie.getValue());
                }

                // Read the response body
                body = EntityUtils.toString(response.getEntity());
            } catch (IOException e) {
                // 网络错误
                status = 3;
            } finally {
                logger.info("调用接口状态：" + status);
            }

        }
        return body;
    }

    /**
     * 创建kafka主题
     * 
     * @param parameters
     * @return
     */
    public String insertMethod(String parameters) {
        String body = null;
        logger.info("parameters:" + parameters);

        if (method != null & parameters != null && !"".equals(parameters.trim())) {
            try {
                // 建立一个NameValuePair数组，用于存储欲传送的参数
                method.addHeader("Content-type", "application/json; charset=utf-8");
                method.setHeader("Accept", "application/json");
                method.setEntity(new StringEntity(parameters, Charset.forName("UTF-8")));
                startTime = System.currentTimeMillis();

                // add zhongsanmu 20171128
                httpClient.setCookieStore(cookieStore);

                HttpResponse response = httpClient.execute(method);

                endTime = System.currentTimeMillis();
                int statusCode = response.getStatusLine().getStatusCode();

                logger.info("statusCode:" + statusCode);
                logger.info("调用API 花费时间(单位：毫秒)：" + (endTime - startTime));
                if (statusCode != HttpStatus.SC_OK) {
                    logger.error("Method failed:" + response.getStatusLine());
                    status = 1;
                }

                CookieStore cookiestore = httpClient.getCookieStore();
                List<Cookie> cookieList = cookiestore.getCookies();
                System.out.println("cookieList.size():" + cookieList.size());
                // 得到Cookie
                StringBuffer tmpcookies = new StringBuffer();
                for (Iterator<Cookie> iterator = cookieList.iterator(); iterator.hasNext();) {
                    Cookie cookie = (Cookie) iterator.next();
                    tmpcookies.append(cookie.toString() + ";");
                    System.out.println("cookies = " + cookie.toString());
                    System.out.println("cookie.getName():" + cookie.getName() + ",cookie.getValue():" + cookie.getValue());
                }

                // Read the response body
                body = EntityUtils.toString(response.getEntity());
            } catch (IOException e) {
                // 网络错误
                status = 3;
            } finally {
                logger.info("调用接口状态：" + status);
            }

        }
        return body;
    }

    public static void main(String[] args) {
        loginMethodTest();
        insertMethodTest();
    }

    public static void loginMethodTest() {
        String loginURL = "http://bd04.guiyang.lgy:50061/v1/user/login";
        HttpTest httpClientTest = new HttpTest(loginURL);
        // JsonArray arry = new JsonArray();
        JsonObject j = new JsonObject();
        j.addProperty("userName", "kafka_test");
        j.addProperty("password", "abc123456");
        // arry.add(j);
        System.out.println(httpClientTest.loginMethod(j.toString()) + "\n\n");
    }

    public static void insertMethodTest() {
        String insertURL = "http://bd04.guiyang.lgy:50061/v1/topic/create";
        // apiURL = "http://bd04.guiyang.lgy:50061/v1/api/topic/create";
        // apiURL = "http://bd04.guiyang.lgy:50061/v1/topic/create";
        HttpTest httpClientTest = new HttpTest(insertURL);
        // String jsonStr001 = "{                " +
        // "\"apiUserName\": \"360admin\", " +
        // "\"apiUserToken\": \"c6f1eea0ab386853415d4953bdd14783\",  " +
        // "\"resource\": { " +
        // "    \"name\": \"test001002\",  " +
        // "    \"comment\": \"test001002\",  " +
        // "    \"source\": { " +
        // "        \"type\": 1 " +
        // "    },  " +
        // "    \"fields\": \"{\\\"fields\\\":[{\\\"name\\\":\\\"test001002\\\",\\\"type\\\":\\\"string\\\"}]}\" " +
        // "},  " +
        // "\"namespace\": \"test001002\",  " +
        // "\"name\": \"test001002\",  " +
        // "\"isSave\": 0" +
        // "}";

        // String jsonStr002 = "{                " +
        // "\"apiUserName\": \"360admin\", " +
        // "\"apiUserToken\": \"c6f1eea0ab386853415d4953bdd14783\",  " +
        // "\"resource\": { " +
        // "    \"name\": \"kaka\",  " +
        // "    \"comment\": \"kaka\",  " +
        // "    \"source\": { " +
        // "        \"type\": 1 " +
        // "    },  " +
        // "    \"fields\": \"{\\\"fields\\\":[{\\\"name\\\":\\\"name\\\",\\\"type\\\":\\\"string\\\"}]}\" " +
        // "},  " +
        // "\"namespace\": \"kaka\",  " +
        // "\"name\": \"kaka\",  " +
        // "\"isSave\": 1,  " +
        // "\"sinks\": [ " +
        // "    { " +
        // "        \"type\": \"Hive\",  " +
        // "        \"path\": \"log\",  " +
        // "        \"db\": \"cache\",  " +
        // "        \"table\": \"log\",  " +
        // "        \"id\": \"\" " +
        // "    } " +
        // "] " +
        // "}";

        // String jsonStr003 = "{" +
        // "\"resource\": {" +
        // "    \"name\": \"test001003\", " +
        // "    \"comment\": \"test001003\", " +
        // "    \"source\": {" +
        // "        \"type\": 1" +
        // "    }, " +
        // "    \"fields\": \"{\\\"fields\\\":[{\\\"name\\\":\\\"test001003\\\",\\\"type\\\":\\\"string\\\"}]}\" " +
        // "}, " +
        // "\"namespace\": \"test001003\", " +
        // "\"name\": \"test001003\", " +
        // "\"isSave\": 0" +
        // "}";

        String jsonStr004 = "{ " +
                "\"resource\": { " +
                "    \"name\": \"test001005test0\",  " +
                "    \"comment\": \"test001005test0\",  " +
                "    \"source\": { " +
                "        \"type\": 1 " +
                "    },  " +
                "    \"fields\": \"{\\\"fields\\\":[{\\\"name\\\":\\\"test001005\\\",\\\"type\\\":\\\"string\\\"}]}\" " +
                "},  " +
                "\"namespace\": \"test001005test0\",  " +
                "\"name\": \"test001005test0\",  " +
                "\"isSave\": 1,  " +
                "\"sinks\": [ " +
                "    { " +
                "        \"type\": \"Hive\",  " +
                "        \"path\": \"test001005\",  " +
                "        \"db\": \"test\",  " +
                "        \"table\": \"test001005\",  " +
                "        \"id\": \"\" " +
                "    } " +
                "] " +
                "}";
        System.out.println(httpClientTest.insertMethod(jsonStr004) + "\n\n");
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }
}

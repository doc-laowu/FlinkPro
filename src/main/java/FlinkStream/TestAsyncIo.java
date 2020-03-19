package FlinkStream;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @Title: TestAsyncIo
 * @ProjectName FlinkPro
 * @Description: TODO
 * @Author yisheng.wu
 * @Date 2020/3/1910:32
 */
public class TestAsyncIo {

    public static class AsyncDatabaseClient {

        private transient SQLClient mySQLClient;

        AsyncDatabaseClient(){
            JsonObject mySQLClientConfig = new JsonObject();
            mySQLClientConfig.put("url", "jdbc:mysql://localhost:3306/yiibaidb?serverTimezone=UTC")
                    .put("driver_class", "com.mysql.cj.jdbc.Driver")
                    .put("max_pool_size", 20)
                    .put("user", "root")
                    .put("password", "123456");

            VertxOptions vo = new VertxOptions();
            vo.setEventLoopPoolSize(10);
            vo.setWorkerPoolSize(20);
            Vertx vertx = Vertx.vertx(vo);
            mySQLClient = JDBCClient.createNonShared(vertx, mySQLClientConfig);
        }

        public void close() throws Exception {
            mySQLClient.close();
        }

        public void query(Integer key, ResultFuture<Tuple2<Integer, String>> resultFuture) {
            mySQLClient.getConnection(conn -> {
                if (conn.failed()) {
                    //Treatment failures
                    return;
                }

                final SQLConnection connection = conn.result();
                connection.query("SELECT lastName, firstName FROM yiibaidb.employees WHERE employeeNumber = " + key, res2 -> {
                    if (res2.succeeded()) {
                        ResultSet rs = res2.result();
                        List<JsonObject> rows = rs.getRows();
                        for (JsonObject json : rows) {
                            resultFuture.complete(Collections.singleton(new Tuple2<>(key, json.toString())));
                        }
                    }
                });

            });
        }
    }

    public static class SyncDatabaseClient{


        private static String jdbcUrl = "jdbc:mysql://localhost:3306/yiibaidb?serverTimezone=UTC";
        private static String username = "root";
        private static String password = "123456";
        private static String driverName = "com.mysql.cj.jdbc.Driver";
        private static java.sql.Connection conn;
        private static PreparedStatement ps;

        SyncDatabaseClient(){
            try {
                Class.forName(driverName);
                conn = DriverManager.getConnection(jdbcUrl, username, password);
                ps = conn.prepareStatement("SELECT lastName, firstName FROM yiibaidb.employees WHERE employeeNumber = ?");
            } catch (ClassNotFoundException | SQLException e) {
                e.printStackTrace();
            }
        }

        public String query(Integer key) {

            JsonObject json = new JsonObject();

            try {
                ps.setInt(1, key.intValue());
                java.sql.ResultSet rs = ps.executeQuery();
                if (!rs.isClosed() && rs.next()) {
                    String lastName = rs.getString("lastName");
                    String firstName = rs.getString("firstName");
                    json.put("lastName", lastName);
                    json.put("firstName", firstName);
                }

            } catch (SQLException e) {
                e.printStackTrace();
            }

            return json.toString();
        }

        public void close() throws SQLException {
            if(ps != null){
                ps.close();
            }
            if(conn != null){
                conn.close();
            }
        }

    }


    /**
     * 实现 'AsyncFunction' 用于发送请求和设置回调。
     */
    public static class AsyncDatabaseRequest extends RichAsyncFunction<Integer, Tuple2<Integer, String>> {

        /** 能够利用回调函数并发发送请求的数据库客户端 */
        private transient AsyncDatabaseClient client;

        private transient SyncDatabaseClient syncClient;

        private transient ExecutorService executorService;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
//            client = new AsyncDatabaseClient();
            syncClient = new SyncDatabaseClient();
            executorService = Executors.newFixedThreadPool(30);
        }

        @Override
        public void close() throws Exception {
            super.close();
//            client.close();
            syncClient.close();
        }

        @Override
        public void asyncInvoke(Integer key, final ResultFuture<Tuple2<Integer, String>> resultFuture) throws Exception {

            // 发送异步请求，等待异步客户端返回结果之后直接发送出去
//            client.query(key, resultFuture);

            // 同步客户端查询数据并返回
            executorService.submit(() -> {
                String ret = syncClient.query(key);
                // 一定要记得放回 resultFuture，不然数据全部是timeout 的
                resultFuture.complete(Collections.singleton(new Tuple2<Integer, String>(key, ret)));
            });

        }

        @Override
        public void timeout(Integer input, ResultFuture<Tuple2<Integer, String>> resultFuture) throws Exception {
            resultFuture.complete(Collections.singleton(new Tuple2<Integer, String>(input, "timeout")));
        }
    }

    public static void main(String[] args) throws Exception {

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<Integer> stream = env
                .socketTextStream("192.168.1.171", 9999)
                .map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.parseInt(value);
                    }
                });

        // 应用异步 I/O 转换操作
        DataStream<Tuple2<Integer, String>> resultStream =
                AsyncDataStream.unorderedWait(stream, new AsyncDatabaseRequest(), 1000, TimeUnit.MILLISECONDS, 100);

        resultStream.print();

        env.execute("AsyncIO test");

    }

}

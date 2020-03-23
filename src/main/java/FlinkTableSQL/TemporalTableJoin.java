package FlinkTableSQL;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @Title: TemporalTableJoin
 * @ProjectName FlinkPro
 * @Description: TODO
 * @Author yisheng.wu
 * @Date 2020/3/2316:02
 */
public class TemporalTableJoin {

    public static void main(String[] args) {

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

    }

}

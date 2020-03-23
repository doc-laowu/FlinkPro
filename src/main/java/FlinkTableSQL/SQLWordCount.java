package FlinkTableSQL;

import Utils.MyTimeUtl;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.DefinedRowtimeAttributes;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.tsextractors.ExistingField;
import org.apache.flink.table.sources.wmstrategies.AscendingTimestamps;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

/**
 * @Title: SQLWordCount
 * @ProjectName FlinkPro
 * @Description: TODO
 * @Author yisheng.wu
 * @Date 2020/3/2015:20
 */
public class SQLWordCount {

    // define a table source with a rowtime attribute
    public static class UserActionSource implements StreamTableSource<Row>, DefinedRowtimeAttributes {

//        @Override
//        public DataType getProducedDataType() {
//
//            return DataTypes.ROW().bridgedTo(Row.class);
//        }

        @Override
        public TypeInformation<Row> getReturnType() {
            String[] names = new String[] {"user_name", "user_action_time"};
            TypeInformation[] types =
                    new TypeInformation[] {Types.STRING(), Types.LONG()};
            return Types.ROW(names, types);
        }

        @Override
        public TableSchema getTableSchema() {

            TableSchema build = TableSchema.builder()
                    .fields(new String[]{"user_name", "user_action_time"},
                            new DataType[]{DataTypes.STRING().bridgedTo(String.class), DataTypes.TIMESTAMP().bridgedTo(Timestamp.class)})
                    .build();

            return build;
        }

        @Override
        public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
            // create stream
            DataStream<Row> dataStream = execEnv
                    .socketTextStream("192.168.1.171", 9999)
                    .flatMap(new FlatMapFunction<String, Row>() {
                        @Override
                        public void flatMap(String sentence, Collector<Row> out) throws Exception {
                            String[] split = sentence.split(";");
                            out.collect(Row.of(split[0], MyTimeUtl.str2milli(split[1])));
                        }
                    });
            // assign watermarks based on the "user_action_time" attribute
            DataStream<Row> stream = dataStream
                    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.minutes(1L)) {
                @Override
                public long extractTimestamp(Row element) {
                    return Long.parseLong(String.valueOf(element.getField(1)));
                }
            }).returns(new RowTypeInfo(Types.STRING(), Types.LONG()));
            return stream;
        }

        @Override
        public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors() {
            // Mark the "user_action_time" attribute as event-time attribute.
            // We create one attribute descriptor of "user_action_time".
            RowtimeAttributeDescriptor rowtimeAttrDescr = new RowtimeAttributeDescriptor(
                    "user_action_time",
                    new ExistingField("user_action_time"),
                    new AscendingTimestamps());
            List<RowtimeAttributeDescriptor> listRowtimeAttrDescr = Collections.singletonList(rowtimeAttrDescr);
            return listRowtimeAttrDescr;
        }
    }

    public static void main(String[] args) throws Exception {

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        env.setMaxParallelism(1);

        EnvironmentSettings setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment stabEnv = StreamTableEnvironment.create(env, setting);

        stabEnv.registerTableSource("user_actions", new UserActionSource());

        Table table = stabEnv
                .from("user_actions")
                .window(Tumble.over("1.minutes").on("user_action_time").as("userActionWindow"))
                .groupBy("userActionWindow, user_name")
                .select("user_name, count(user_name) as cnt, userActionWindow.start as start_time");


//        Table select = stabEnv.from("user_actions");
//        Table table = stabEnv.sqlQuery("select \n" +
//                "user_name, \n" +
//                "count(user_name) as cnt, \n" +
//                "TUMBLE_START(user_action_time, INTERVAL '1' MINUTE) as start_time\n" +
//                "from user_actions\n" +
//                "group by TUMBLE(user_action_time, INTERVAL '1' MINUTE), user_name");

        DataStream<Row> rowDataStream = stabEnv.toAppendStream(table,
                Types.ROW(new String[]{"user_name", "cnt", "start_time"}, new TypeInformation[]{Types.STRING(), Types.LONG(), Types.SQL_TIMESTAMP()}));

        rowDataStream.print();

        env.execute();
    }
}

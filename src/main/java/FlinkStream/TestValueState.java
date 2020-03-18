package FlinkStream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Title: TestValueState
 * @ProjectName FlinkPro
 * @Description: TODO 测试ValueState
 * @Author yisheng.wu
 * @Date 2020/3/1817:28
 */
public class TestValueState {

    public static class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

        /**
         * The ValueState handle. The first field is the count, the second field a running sum.
         */
        private transient ValueState<Tuple2<Long, Long>> sum;

        @Override
        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

            // access the state value
            Tuple2<Long, Long> currentSum = sum.value();

            // update the count
            currentSum.f0 += 1;

            // add the second field of the input value
            currentSum.f1 += input.f1;

            // update the state
            sum.update(currentSum);

            // if the count reaches 2, emit the average and clear the state
            if (currentSum.f0 >= 2) {
                out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
                sum.clear();
            }
        }

        @Override
        public void open(Configuration config) {

            /**
             * 状态上次的修改时间会和数据一起保存在 state backend 中，因此开启该特性会增加状态数据的存储。 Heap state backend 会额外存储一个包括用户状态以及时间戳的 Java 对象，RocksDB state backend 会在每个状态值（list 或者 map 的每个元素）序列化后增加 8 个字节。
             *
             * 暂时只支持基于 processing time 的 TTL。
             *
             * 尝试从 checkpoint/savepoint 进行恢复时，TTL 的状态（是否开启）必须和之前保持一致，否则会遇到 “StateMigrationException”。
             *
             * TTL 的配置并不会保存在 checkpoint/savepoint 中，仅对当前 Job 有效。
             *
             * 当前开启 TTL 的 map state 仅在用户值序列化器支持 null 的情况下，才支持用户值为 null。如果用户值序列化器不支持 null， 可以用 NullableSerializer 包装一层。
             */

            /**
             设置状态的ttl
             **/
            StateTtlConfig ttlConfig = StateTtlConfig
                    /**
                     newBuilder 的第一个参数表示数据的有效期，是必选项
                     **/
                    .newBuilder(Time.seconds(1))
                    /**
                     TTL 的更新策略（默认是 OnCreateAndWrite）：

                     StateTtlConfig.UpdateType.OnCreateAndWrite - 仅在创建和写入时更新
                     StateTtlConfig.UpdateType.OnReadAndWrite - 读取时也更新
                     **/
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    /**
                     数据在过期但还未被清理时的可见性配置如下（默认为 NeverReturnExpired):

                     StateTtlConfig.StateVisibility.NeverReturnExpired - 不返回过期数据
                     StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp - 会返回过期但未清理的数据
                     NeverReturnExpired 情况下，过期数据就像不存在一样，不管是否被物理删除。这对于不能访问过期数据的场景下非常有用，比如敏感数据。 ReturnExpiredIfNotCleanedUp 在数据被物理删除前都会返回。
                     */
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    /**
                     * 另外，你可以启用全量快照时进行清理的策略，这可以减少整个快照的大小。当前实现中不会清理本地的状态，但从上次快照恢复时，不会恢复那些已经删除的过期数据。 该策略可以通过 StateTtlConfig 配置进行配置：
                     *
                     */
                    .cleanupFullSnapshot()
                    /**
                     * 该策略有两个参数。 第一个是每次清理时检查状态的条目数，在每个状态访问时触发。第二个参数表示是否在处理每条记录时触发清理。 Heap backend 默认会检查 5 条状态，并且关闭在每条记录时触发清理。
                     *
                     * 注意:
                     *
                     * 如果没有 state 访问，也没有处理数据，则不会清理过期数据。
                     * 增量清理会增加数据处理的耗时。
                     * 现在仅 Heap state backend 支持增量清除机制。在 RocksDB state backend 上启用该特性无效。
                     * 如果 Heap state backend 使用同步快照方式，则会保存一份所有 key 的拷贝，从而防止并发修改问题，因此会增加内存的使用。但异步快照则没有这个问题。
                     * 对已有的作业，这个清理方式可以在任何时候通过 StateTtlConfig 启用或禁用该特性，比如从 savepoint 重启后。
                     */
                    .cleanupIncrementally(10, true)
                    /**\
                     * Flink 处理一定条数的状态数据后，会使用当前时间戳来检测 RocksDB 中的状态是否已经过期， 你可以通过 StateTtlConfig.newBuilder(...).cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries) 方法指定处理状态的条数。
                     * 时间戳更新的越频繁，状态的清理越及时，但由于压缩会有调用 JNI 的开销，因此会影响整体的压缩性能。 RocksDB backend 的默认后台清理策略会每处理 1000 条数据进行一次。
                     *
                     * 你还可以通过配置开启 RocksDB 过滤器的 debug 日志： log4j.logger.org.rocksdb.FlinkCompactionFilter=DEBUG
                     *
                     * 注意:
                     *
                     * 压缩时调用 TTL 过滤器会降低速度。TTL 过滤器需要解析上次访问的时间戳，并对每个将参与压缩的状态进行是否过期检查。 对于集合型状态类型（比如 list 和 map），会对集合中每个元素进行检查。
                     * 对于元素序列化后长度不固定的列表状态，TTL 过滤器需要在每次 JNI 调用过程中，额外调用 Flink 的 java 序列化器， 从而确定下一个未过期数据的位置。
                     * 对已有的作业，这个清理方式可以在任何时候通过 StateTtlConfig 启用或禁用该特性，比如从 savepoint 重启后。
                     */
                    .cleanupInRocksdbCompactFilter(1000)
                    .build();

            ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                    new ValueStateDescriptor<>(
                            "average", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                            Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
            /**
             应用到状态上
             **/
            descriptor.enableTimeToLive(ttlConfig);

            sum = getRuntimeContext().getState(descriptor);
        }
    }

    public static void main(String[] args) throws Exception {

        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(0)
                .flatMap(new CountWindowAverage())
                .print();

        env.execute();

    }

}

package FlinkStream;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @Title: BufferingSink
 * @ProjectName FlinkPro
 * @Description: TODO 下面的例子中的 SinkFunction 在 CheckpointedFunction 中进行数据缓存，然后统一发送到下游，这个例子演示了列表状态数据的 event-split redistribution。
 * @Author yisheng.wu
 * @Date 2020/3/1818:20
 */
public class BufferingSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

    /**
     * 当前，managed operator state 以 list 的形式存在。这些状态是一个 可序列化 对象的集合 List，彼此独立，方便在改变并发后进行状态的重新分派。 换句话说，这些对象是重新分配 non-keyed state 的最细粒度。根据状态的不同访问方式，有如下几种重新分配的模式：
     *
     * Even-split redistribution: 每个算子都保存一个列表形式的状态集合，整个状态由所有的列表拼接而成。当作业恢复或重新分配的时候，整个状态会按照算子的并发度进行均匀分配。 比如说，算子 A 的并发读为 1，包含两个元素 element1 和 element2，当并发读增加为 2 时，element1 会被分到并发 0 上，element2 则会被分到并发 1 上。
     *
     * Union redistribution: 每个算子保存一个列表形式的状态集合。整个状态由所有的列表拼接而成。当作业恢复或重新分配时，每个算子都将获得所有的状态数据。
     */

    private final int threshold;

    private transient ListState<Tuple2<String, Integer>> checkpointedState;

    private List<Tuple2<String, Integer>> bufferedElements;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    /**
     Writes the given value to the sink. This function is called for every record.
     执行写入的地方
     **/
    @Override
    public void invoke(Tuple2<String, Integer> value, Context contex) throws Exception {
        bufferedElements.add(value);
        if (bufferedElements.size() == threshold) {
            for (Tuple2<String, Integer> element: bufferedElements) {
                // send it to the sink
            }
            bufferedElements.clear();
        }
    }

    /**
     * 进行 checkpoint 时会调用 snapshotState()。
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        /**
         在快照的时候会使用之前初始化好的状态数，所以需要先清空 ListState，再把当前局部变量中的所有元素写入到 checkpoint 中
         **/
        checkpointedState.clear();
        for (Tuple2<String, Integer> element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    /**
     * 用户自定义函数初始化时会调用 initializeState()，初始化包括第一次自定义函数初始化和从之前的 checkpoint 恢复。
     * 因此 initializeState() 不仅是定义不同状态类型初始化的地方，也需要包括状态恢复的逻辑。
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                new ListStateDescriptor<>(
                        "buffered-elements",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

        /**
         调用不同的获取状态对象的接口，会使用不同的状态分配算法。比如 getUnionListState(descriptor) 会使用 union redistribution 算法，
         而 getListState(descriptor) 则简单的使用 even-split redistribution 算法。
         **/
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        /**
         * 当初始化好状态对象后，我们通过 isRestored() 方法判断是否从之前的故障中恢复回来，如果该方法返回 true 则表示从故障中进行恢复，会执行接下来的恢复逻辑。
         */
        if (context.isRestored()) {
            /**
             * 正如代码所示，BufferingSink 中初始化时，恢复回来的 ListState 的所有元素会添加到一个局部变量中，供下次 snapshotState() 时使用。
             * 然后清空 ListState，再把当前局部变量中的所有元素写入到 checkpoint 中。
             */
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }
}

package FlinkStream;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Collections;
import java.util.List;

/**
 * @Title: CounterSource
 * @ProjectName FlinkPro
 * @Description: TODO ListCheckpointed 接口是 CheckpointedFunction 的精简版，仅支持 even-split redistributuion 的 list state。
 * @Author yisheng.wu
 * @Date 2020/3/1818:45
 */
public class CounterSource extends RichParallelSourceFunction<Long> implements ListCheckpointed<Long> {

    /**
     * 带状态的数据源比其他的算子需要注意更多东西。为了保证更新状态以及输出的原子性（用于支持 exactly-once 语义），用户需要在发送数据前获取数据源的全局锁。
     */


    /**  current offset for exactly once semantics */
    private Long offset = 0L;

    /** flag for job cancellation */
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) {
        final Object lock = ctx.getCheckpointLock();

        while (isRunning) {
            // output and state update are atomic
            synchronized (lock) {
                ctx.collect(offset);
                offset += 1;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    /**\
     * snapshotState() 需要返回一个将写入到 checkpoint 的对象列表。如果状态不可切分， 则可以在 snapshotState() 中返回 Collections.singletonList(MY_STATE)。
     */
    @Override
    public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) {
        return Collections.singletonList(offset);
    }

    /**\
     * restoreState 则需要处理恢复回来的对象列表
     */
    @Override
    public void restoreState(List<Long> state) {
        for (Long s : state)
            offset = s;
    }
}

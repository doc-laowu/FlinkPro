package Utils;

import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;

/**
 * @Title: MyTimeUtl
 * @ProjectName FlinkPro
 * @Description: TODO
 * @Author yisheng.wu
 * @Date 2020/3/2015:22
 */
public class MyTimeUtl {

    private static final FastDateFormat fdf = FastDateFormat.getInstance("yyy-MM-dd HH:mm:ss");

    public static long str2milli(String timeStr) throws ParseException {
        return fdf.parse(timeStr).getTime();
    }

}

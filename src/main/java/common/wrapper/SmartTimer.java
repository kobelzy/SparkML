package common.wrapper;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 时间工具类
 * 2016年7月8日
 * licheng
 * v1.3.1
 */
public class SmartTimer {
    //秒
    public static final int MILLIS_SECOND = 1000;
    //分钟
    public static final int MILLIS_MINUTE = 1000 * 60;
    //小时
    public static final int MILLIS_HOUR = 1000 * 60 * 60;
    //天
    public static final int MILLIS_DAY = 1000 * 60 * 60 * 24;

    public static Calendar getCalendar() {
        return Calendar.getInstance();
    }

    public static SimpleDateFormat sdf(String format) {
        return new SimpleDateFormat(format);
    }

    /**
     * 日期转String
     * Param:
     * format 日期格式 yyyy-MM-dd HH:mm:ss
     * Return:
     * Created by lzy on 2017/6/7.
     */
    public static String format(Date date, String format) {
        return sdf(format).format(date);
    }

    /**
     * String转日期,补充缺少的秒单位
     * Param:
     * Return:
     * Created by lzy on 2017/8/3.
     */
    public static Date format(String time, String format) {
        String[] timeArr = time.split(" ");
        //如果format格式带秒，time中缺少秒，即用:00补齐
        if (timeArr.length == 2) {
            String _time = timeArr[1];
            if (_time.split(":").length == 2) {
                time += ":00";
            }
        }
        Date date = null;
        try {
            date = sdf(format).parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date;
    }

    /**
     * 获取当前日期
     *
     * @param format 日期格式 yyyy-MM-dd HH:mm:ss
     * @return
     */
    public static String getCurrentTime(String format) {
        return format(new Date(), format);
    }

    /**
     * 获取当前日期
     *
     * @return
     */
    public static String getCurrentTime() {
        return getCurrentTime("yyyy-MM-dd");
    }

    /**
     * 每天定时执行(阻塞)
     * Param: runTime 运行时间 ("HH:mm:ss")
     * Return:
     * Created by lzy on 2017/12/7.
     */
    public static void everyDayBlock(String runTime, Runnable runnable) {
        Calendar calendar = Calendar.getInstance();
        long currentMillis = calendar.getTimeInMillis();
        String[] runTime_ = runTime.split(":");
        int hour = Integer.parseInt(runTime_[0]);
        int minute = Integer.parseInt(runTime_[1]);
        int second = Integer.parseInt(runTime_[2]);
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), hour, minute, second);
        long runMillis = calendar.getTimeInMillis();
        long gap = runMillis - currentMillis;
        if (gap <= 0) {
            //如果时间点已过，将在第二天的该时间点执行
            gap += MILLIS_DAY;
        }
        try {
            Thread.sleep(gap);
        } catch (Exception e) {
            e.printStackTrace();
        }
        runnable.run();
        everyDayBlock(runTime, runnable);
    }

    /**
     * 每天定时执行(非阻塞)
     * Param: runTime 运行时间 ("HH:mm:ss")
     * Return:
     * Created by lzy on 2017/12/7.
     */
    public static void everyDay(final String runTime, final Runnable runnable) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                everyDayBlock(runTime, runnable);
            }
        }).start();
    }

    /**
     * 时间转化为毫秒
     * Param:
     * format: 日期格式 yyyy-MM-dd HH:mm:ss
     * Return:
     * Created by lzy on 2018/6/7.
     */
    public static long toMillis(String time, String format) {
        long millis = 0;
        SimpleDateFormat sdf = sdf(format);
        try {
            millis = sdf.parse(time).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return millis;
    }

    /**为指定日期加上对应的毫秒数，返回新的时间戳，输入与返回的时间格式都应与format相同
     * Param:
     * startTime: 开始时间
     * millis: 相间毫秒数
     * format: 当前输入时间的日期格式 yyyy-MM-dd HH:mm:ss
     * Return: 返回一个相加后的日期类型
     * Created by lzy on 2018/6/7.
     */
    public static String addMillis(String startTime, int millis, String format) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(format(startTime, format));
        calendar.add(Calendar.MILLISECOND, millis);
        return format(calendar.getTime(), format);
    }

    /**
     * 时间做减，计算相差毫秒数
     * Param:
     * format: 日期格式 yyyy-MM-dd HH:mm:ss
     * Return:
     * Created by licheng on 2017/6/7.
     */
    public static long subtractMillis(String startTime, String endTime, String format) {
        long startMillis = 0;
        long endMillis = 0;
        try {
            startMillis = toMillis(startTime, format);
            endMillis = toMillis(endTime, format);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return endMillis - startMillis;
    }

    /**
     * @param format: 日期格式 yyyy-MM-dd HH:mm:ss
     * @return 距今天的天数
     */
    public static long toDaysNum(String startTime, String format) {
        return subtractMillis(startTime, getCurrentTime(), format) / MILLIS_DAY;
    }

    /**
     * 获取据当前时间指定天数的时间
     * Param: count: eg: -1 即获取昨天的日期
     * Return:
     * Created by lzy on 2018/4/4.
     */
    public static Calendar toDay(int count) {
        Calendar calendar = getCalendar();
        calendar.add(Calendar.DATE, count);
        return calendar;
    }

    /**
     * 获取据当前时间指定月数的时间
     * Param:eg: -1 即获取上个月的日期
     * Return:
     * Created by lzy on 2018/4/4.
     */
    public static Calendar toMonth(int count) {
        Calendar calendar = getCalendar();
        calendar.add(Calendar.MONTH, count);
        return calendar;
    }



    public static void main(String[] args) {
       /*Calendar calendar = Calendar.getInstance();
       calendar.add(Calendar.MONTH,-1);*/
//        Calendar cal = SmartTimer.toMonth(-1);
//        String a = SmartTimer.format(cal.getTime(), "yyyy-MM-dd");
//        System.out.println(a);
    }

}

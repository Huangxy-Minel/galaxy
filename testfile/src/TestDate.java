package galaxy.testfile;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

public class TestDate {
    public static void main(String[] args) throws Exception {
        Date date=new Date();
        DateFormat format=new SimpleDateFormat("yyyyMMdd");
        System.out.println(format.format(date));
    }
}
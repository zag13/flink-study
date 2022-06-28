package com.github.zag13.util;

import java.util.Properties;

public class PropertyUtil {

    public synchronized static Properties getProperties(String filename) {
        Properties result = new Properties();
        try {
            result.load(PropertyUtil.class.getClassLoader().getResourceAsStream(filename));
        } catch (Exception e) {
            //没加载到文件，程序要考虑退出
            e.printStackTrace();
        }
        return result;
    }

}

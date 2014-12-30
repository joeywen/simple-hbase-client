package com.joey.hbase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author joey.wen
 * @date 2014/12/30
 */
public class ConfigLoader {

    private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    private static Properties p = new Properties();

    public static void load() {
        try {
            // Double check
            if (p.size() == 0) {
                synchronized (ConfigLoader.class) {
                    if (p.size() == 0) {
                        p.load(ConfigLoader.class.getResourceAsStream("/config.properties"));
                    }
                }
            }
        } catch (Exception e) {
            logger.error("config.properties file is missing");
        }
    }

    private static String getProperty(String name){
        return p.getProperty(name);
    }

    /**
     * getProperty方法的简写
     * @param name
     * @return value
     */
    public static String get(String name) {
        return getProperty(name);
    }

    /**
     * 获取一个配制项，如果项没有被配制，则返回设置的默认值
     * @param name 配制项名
     * @param defaultValue 默认值
     * @return 配制值
     */
    public static String get(String name, String defaultValue) {
        String ret = getProperty(name);
        return ret == null ? defaultValue : ret;
    }
}

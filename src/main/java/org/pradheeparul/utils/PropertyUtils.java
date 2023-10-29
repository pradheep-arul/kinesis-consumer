package org.pradheeparul.utils;

import java.util.ResourceBundle;

/**
 * PropertyUtils
 *
 * @author PradheepKumarA
 */
public class PropertyUtils {
    public static String getProperty(String config, String propertyName) {
        return ResourceBundle.getBundle(config).getString(propertyName);
    }

    public static int getIntegerProperty(String config, String propertyName) {
        return Integer.parseInt(ResourceBundle.getBundle(config).getString(propertyName));
    }
}

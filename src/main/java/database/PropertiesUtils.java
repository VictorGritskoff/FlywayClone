package database;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtils {
    private static final Properties properties = new Properties();

    private PropertiesUtils() {
    }

    static {
        try (InputStream input = PropertiesUtils.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new FileNotFoundException("Файл application.properties не найден!");
            }
            properties.load(input);
        } catch (IOException ex) {
            System.err.println("Ошибка при чтении/записи в файл: " + ex.getMessage());
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

}

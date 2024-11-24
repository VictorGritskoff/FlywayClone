package utils;

import lombok.extern.slf4j.Slf4j;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Утилитный класс для работы с файлом конфигурации {@code application.properties}.
 * <p>
 * Класс предоставляет методы для получения свойств из файла конфигурации.
 * При загрузке файла значения свойств могут содержать плейсхолдеры в формате
 * {@code {имя_переменной}}, которые заменяются значениями системных переменных окружения.
 * </p>
 *
 * <p><b>Пример использования:</b></p>
 * <pre>{@code
 * String dbUrl = PropertiesUtils.getProperty("database.url");
 * }</pre>
 */

@Slf4j
public class PropertiesUtils {
    /**
     * Хранилище свойств, загружаемых из файла {@code application.properties}.
     */
    private static final Properties properties = new Properties();

    private PropertiesUtils() {
    }

    /**
     * Статический блок инициализации. Загружает свойства из файла {@code application.properties}.
     * Выполняет замену плейсхолдеров значениями системных переменных окружения.
     * <p>
     * Если файл не найден, генерируется исключение {@link FileNotFoundException},
     * которое логируется с использованием библиотеки SLF4J.
     * </p>
     */
    static {
        try (InputStream input = PropertiesUtils.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new FileNotFoundException("Файл application.properties не найден!");
            }
            properties.load(input);
            resolvePlaceholders();
        } catch (IOException ex) {
            log.error("Ошибка при чтении из файла: " + ex.getMessage());
        }
    }

    /**
     * Выполняет замену плейсхолдеров в формате {@code {имя_переменной}} на значения
     * соответствующих системных переменных окружения.
     * <p>
     * Этот метод используется только при загрузке свойств.
     * </p>
     */
    private static void resolvePlaceholders() {
        properties.forEach((key, value) -> {
            String stringValue = (String) value;
            String resolvedValue = resolveValue(stringValue);
            properties.setProperty((String) key, resolvedValue);
        });
    }

    /**
     * Выполняет поиск и замену плейсхолдера в строке на значение переменной окружения.
     *
     * @param value строка, содержащая плейсхолдер в формате {@code {имя_переменной}}.
     * @return строка с замененным плейсхолдером, или исходная строка, если плейсхолдер не найден
     * или соответствующая переменная окружения не определена.
     */
    private static String resolveValue(String value) {
        if (value == null) {
            return null;
        }

        int startIdx = value.indexOf('{');
        int endIdx = value.indexOf('}');
        if (startIdx >= 0 && endIdx > startIdx) {
            String placeholder = value.substring(startIdx + 1, endIdx);
            String envValue = System.getenv(placeholder);
            if (envValue != null) {
                return value.substring(0, startIdx) + envValue + value.substring(endIdx + 1);
            }
        }
        return value;
    }

    /**
     * Получает значение свойства по ключу.
     *
     * @param key ключ свойства.
     * @return значение свойства.
     * @throws IllegalArgumentException если свойство с указанным ключом не найдено.
     */
    public static String getProperty(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            throw new IllegalArgumentException("Свойство с ключом '" + key + "' не найдено!");
        }
        return value;
    }

    /**
     * Получает значение свойства по ключу с возможностью указания значения по умолчанию.
     *
     * @param key ключ свойства.
     * @param defaultValue значение по умолчанию, возвращаемое, если свойство не найдено.
     * @return значение свойства, или значение по умолчанию, если свойство не найдено.
     */
    public static String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}

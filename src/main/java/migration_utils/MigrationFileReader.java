package migration_utils;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Класс, реализующий методы для работы с миграционными файлами:
 * <ul>
 *     <li>Поиск миграционных файлов с заданным форматом имени.</li>
 *     <li>Извлечение версии миграции из имени файла.</li>
 *     <li>Чтение SQL-скрипта из миграционного файла.</li>
 * </ul>
 * <p>
 * Все миграционные файлы должны быть в формате "V<номер версии>__<описание>.sql", где:
 * <ul>
 *     <li><b><номер версии></b> — целое число, которое обозначает уникальный номер миграции,</li>
 *     <li><b><описание></b> — произвольное описание миграции.</li>
 * </ul>
 * </p>
 */

public class MigrationFileReader {

    /**
     * Находит и возвращает список файлов миграций в указанной директории.
     * Файлы должны соответствовать формату "V<номер версии>__<описание>.sql".
     *
     * @param "directoryPath" Путь к директории, содержащей файлы миграций.
     * @return Список файлов миграций, отсортированных по версии.
     * @throws IllegalArgumentException Если директория не существует или не является папкой,
     *                                   или если произошла ошибка при доступе к ресурсам.
     */

    public static List<File> findMigrationFiles(String directoryPath) {
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            URL resourceUrl = classLoader.getResource(directoryPath);

            if (resourceUrl == null) {
                throw new IllegalArgumentException("Ресурс не найден: " + directoryPath);
            }

            File directory = new File(resourceUrl.toURI());

            if (!directory.exists() || !directory.isDirectory()) {
                throw new IllegalArgumentException("Каталог миграций не найден: " + directoryPath);
            }

            return Arrays.stream(Objects.requireNonNull(directory.listFiles()))
                    .filter(file -> file.getName().matches("V\\d+__.*\\.sql"))
                    .sorted(Comparator.comparing(MigrationFileReader::getVersionFromFile))
                    .toList();
        } catch (URISyntaxException | NullPointerException e) {
            throw new IllegalArgumentException("Ошибка при доступе к директории миграций: " + directoryPath, e);
        }
    }


    /**
     * Извлекает версию миграции из имени файла.
     * Формат имени файла должен быть "V<номер версии>__<описание>.sql".
     *
     * @param migrationFile Файл миграции.
     * @return Номер версии миграции (число).
     */

    public static String getVersionFromFile(File migrationFile) {
        return migrationFile.getName().split("__")[0].substring(1);
    }

    /**
     * Читает SQL-скрипт из файла миграции и возвращает его в виде строки.
     *
     * @param migrationFile Файл миграции.
     * @return Строка с SQL-скриптом.
     */

    public static String readSqlFromFile(File migrationFile) {
        StringBuilder sqlBuilder = new StringBuilder();

        try (FileInputStream fis = new FileInputStream(migrationFile)) {
            byte[] data = new byte[(int) migrationFile.length()];
            fis.read(data);
            sqlBuilder.append(new String(data, StandardCharsets.UTF_8));
        } catch (IOException e) {
            System.err.println("Ошибка при чтении файла миграции: " + migrationFile.getName());
            e.printStackTrace();
            return "";
        }
        return sqlBuilder.toString();
    }

}

package migration_utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MigrationFileReader {

    public static List<File> findMigrationFiles(String migrationsPath) {
        List<File> migrationFiles = new ArrayList<>();

        try {
            File migrationsDirectory = new File(migrationsPath);

            if (!migrationsDirectory.exists() || !migrationsDirectory.isDirectory()) {
                System.err.println("Ошибка: путь не существует или не является директорией: " + migrationsPath);
                return migrationFiles;
            }

            File[] files = migrationsDirectory.listFiles((dir, name) -> name.endsWith(".sql"));

            if (files != null) {
                Collections.addAll(migrationFiles, files);
            }

            migrationFiles.sort((f1, f2) -> {
                try {
                    String version1 = f1.getName().split("__")[0].substring(1);
                    String version2 = f2.getName().split("__")[0].substring(1);
                    return Double.compare(Double.parseDouble(version1), Double.parseDouble(version2));
                } catch (Exception e) {
                    System.err.println("Ошибка при сортировке файлов миграции: " + e.getMessage());
                    e.printStackTrace();
                    return 0;
                }
            });
        } catch (Exception e) {
            System.err.println("Ошибка при поиске файлов миграции: " + e.getMessage());
            e.printStackTrace();
        }

        return migrationFiles;
    }

    public static double getVersionFromFile(File migrationFile) {
        String fileName = migrationFile.getName();
        String versionString = fileName.split("__")[0].substring(1);
        return Double.parseDouble(versionString);
    }

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

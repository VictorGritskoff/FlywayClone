package report;

import lombok.extern.slf4j.Slf4j;
import utils.PropertiesUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

/**
 * Класс для генерации отчетов о миграциях базы данных.
 * Поддерживает экспорт данных о миграциях в форматы CSV и JSON.
 * В отчете содержатся следующие поля: id, version, description, status, reverted, applied_at.
 */
@Slf4j
public class MakeReport {

    private static final String REPORTS_DIRECTORY = "reports";
    private static final String QUERY = "SELECT id, version, description, status, reverted, applied_at FROM migration_history";

    /**
     * Экспортирует данные о миграциях в формат CSV.
     *
     * @param fileName Имя файла для сохранения отчета (с расширением .csv).
     */
    public static void exportCsv(String fileName) {
        String filePath = prepareFilePath(fileName);
        if (filePath == null) return;

        try (ResultSet resultSet = fetchMigrationHistory();
             FileWriter fileWriter = new FileWriter(filePath)) {

            fileWriter.append("id,version,description,status,reverted,applied_at\n");

            while (resultSet.next()) {
                fileWriter.append(String.valueOf(resultSet.getInt("id")))
                        .append(',')
                        .append(resultSet.getString("version"))
                        .append(',')
                        .append(resultSet.getString("description") != null ? resultSet.getString("description") : "")
                        .append(',')
                        .append(resultSet.getBoolean("status") ? "true" : "false")
                        .append(',')
                        .append(resultSet.getBoolean("reverted") ? "true" : "false")
                        .append(',')
                        .append(resultSet.getTimestamp("applied_at").toString())
                        .append('\n');
            }

            log.info("Данные успешно экспортированы в файл {}", filePath);

        } catch (SQLException | IOException e) {
            log.error("Ошибка при экспорте данных в CSV: {}", e.getMessage(), e);
        }
    }

    /**
     * Экспортирует данные о миграциях в формат JSON.
     *
     * @param fileName Имя файла для сохранения отчета (с расширением .json).
     */
    public static void exportJson(String fileName) {
        String filePath = prepareFilePath(fileName);
        if (filePath == null) return;

        try (ResultSet resultSet = fetchMigrationHistory();
             FileWriter fileWriter = new FileWriter(filePath)) {

            fileWriter.append("[\n");

            boolean isFirst = true;
            while (resultSet.next()) {
                if (!isFirst) {
                    fileWriter.append(",\n");
                }
                isFirst = false;

                fileWriter.append("  {")
                        .append("\"id\": ").append(String.valueOf(resultSet.getInt("id"))).append(", ")
                        .append("\"version\": \"").append(resultSet.getString("version")).append("\", ")
                        .append("\"description\": \"")
                        .append(resultSet.getString("description") != null ? resultSet.getString("description") : "")
                        .append("\", ")
                        .append("\"status\": ").append(resultSet.getBoolean("status") ? "true" : "false").append(", ")
                        .append("\"reverted\": ").append(resultSet.getBoolean("reverted") ? "true" : "false").append(", ")
                        .append("\"applied_at\": \"").append(resultSet.getTimestamp("applied_at").toString()).append("\"")
                        .append("}");
            }

            fileWriter.append("\n]\n");

            log.info("Данные успешно экспортированы в JSON-файл {}", filePath);

        } catch (SQLException | IOException e) {
            log.error("Ошибка при экспорте данных в JSON: {}", e.getMessage(), e);
        }
    }

    /**
     * Подготавливает путь к файлу для сохранения отчета.
     * Если директория для отчетов не существует, она будет создана.
     *
     * @param fileName Имя файла для отчета.
     * @return Путь к файлу или null, если не удалось создать директорию.
     */
    private static String prepareFilePath(String fileName) {
        String reportsDirPath = new File(REPORTS_DIRECTORY).getAbsolutePath();
        File reportsDir = new File(reportsDirPath);

        if (!reportsDir.exists() && !reportsDir.mkdirs()) {
            log.error("Не удалось создать директорию {}", reportsDirPath);
            return null;
        }

        return reportsDirPath + File.separator + fileName;
    }

    /**
     * Выполняет запрос к базе данных для получения истории миграций.
     *
     * @return Результат запроса, содержащий историю миграций.
     * @throws SQLException Если не удалось выполнить запрос к базе данных.
     */
    private static ResultSet fetchMigrationHistory() throws SQLException {
        Connection connection = DriverManager.getConnection(
                PropertiesUtils.getProperty("db.url"),
                PropertiesUtils.getProperty("db.username"),
                PropertiesUtils.getProperty("db.password"));

        Statement statement = connection.createStatement();
        return statement.executeQuery(QUERY);
    }
}

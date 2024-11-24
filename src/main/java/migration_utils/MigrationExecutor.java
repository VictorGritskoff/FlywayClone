package migration_utils;


import java.io.File;
import java.sql.*;

/**
 * Класс для выполнения миграций в базе данных.
 * Содержит метод для выполнения SQL-запросов из файла и записи информации о выполненной миграции в таблицу {@code migration_history}.
 */

public class MigrationExecutor {
    private static final String EXECUTE_NEW_MIGRATION = """
                    INSERT INTO migration_history (version, description, status, reverted, applied_at)
                    VALUES (?, ?, ?, FALSE, CURRENT_TIMESTAMP)
                    ON CONFLICT (version) DO UPDATE SET
                        description = EXCLUDED.description,
                        status = EXCLUDED.status,
                        reverted = FALSE,
                        applied_at = CASE WHEN migration_history.reverted = TRUE THEN CURRENT_TIMESTAMP ELSE migration_history.applied_at END;
                """;

    /**
     * Выполняет миграцию:
     * <ol>
     *   <li>Выполняет SQL-запрос из файла миграции.
     *   <li>Добавляет запись о выполненной миграции в таблицу {@code migration_history}.
     * <ol/>
     *
     * @param connection Соединение с базой данных, используемое для выполнения SQL-запросов.
     * @param file       Файл миграции, содержащий SQL-запрос для выполнения.
     * @throws SQLException Если возникает ошибка при выполнении SQL-запросов.
     */
    public static void execute(Connection connection, File file) throws SQLException {
        String sql = MigrationFileReader.readSqlFromFile(file);
        String version = MigrationFileReader.getVersionFromFile(file);
        String description = "Migration " + file.getName();

        // Выполнение SQL-запроса миграции
        executeSqlStatement(connection, sql);

        // Запись миграции в историю
        recordMigration(connection, version, description, true);
    }

    /**
     * Выполняет SQL-запрос.
     *
     * @param connection Соединение с базой данных.
     * @param sql        SQL-запрос для выполнения.
     * @throws SQLException Если возникает ошибка при выполнении запроса.
     */
    private static void executeSqlStatement(Connection connection, String sql) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeUpdate();
        }
    }

    /**
     * Добавляет запись о выполненной миграции в таблицу {@code migration_history}.
     *
     * @param connection Соединение с базой данных.
     * @param version    Версия миграции.
     * @param description Описание миграции.
     * @param status     Статус миграции (true — выполнена успешно).
     * @throws SQLException Если возникает ошибка при выполнении запроса.
     */
    private static void recordMigration(Connection connection, String version, String description, boolean status) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(EXECUTE_NEW_MIGRATION)) {
            statement.setString(1, version);
            statement.setString(2, description);
            statement.setBoolean(3, status);
            statement.executeUpdate();
        }
    }
}

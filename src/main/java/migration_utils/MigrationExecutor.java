package migration_utils;


import java.io.File;
import java.sql.*;

/**
 * Класс для выполнения миграций в базе данных.
 * Содержит метод для выполнения SQL-запросов из файла и записи информации о выполненной миграции в таблицу {@code migration_history}.
 */

public class MigrationExecutor {

    /**
     * Выполняет миграцию:
     * 1. Выполняет SQL-запрос из файла миграции.
     * 2. Добавляет запись о выполненной миграции в таблицу {@code migration_history}.
     * <p>
     * Этот метод использует два запроса:
     * <ol>
     *   <li>Первый выполняет SQL-запрос из файла миграции, который может изменять структуру базы данных.</li>
     *   <li>Второй записывает информацию о выполненной миграции в таблицу {@code migration_history}, обновляя описание и статус миграции, если она уже существует.</li>
     * </ol>
     *
     * @param connection Соединение с базой данных, используемое для выполнения SQL-запросов.
     * @param file Файл миграции, содержащий SQL-запрос для выполнения.
     * @throws SQLException Если возникает ошибка при выполнении SQL-запросов.
     */

    public static void execute(Connection connection, File file) throws SQLException {
        String sql = MigrationFileReader.readSqlFromFile(file);

        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeUpdate();
        }

        String newMigration = """
                    INSERT INTO migration_history (version, description, status, reverted, applied_at)
                    VALUES (?, ?, ?, FALSE, CURRENT_TIMESTAMP)
                    ON CONFLICT (version) DO UPDATE SET
                        description = EXCLUDED.description,
                        status = EXCLUDED.status,
                        reverted = FALSE,
                        applied_at = CASE WHEN migration_history.reverted = TRUE THEN CURRENT_TIMESTAMP ELSE migration_history.applied_at END;
                """;
        try (PreparedStatement statement = connection.prepareStatement(newMigration)) {
            statement.setString(1, MigrationFileReader.getVersionFromFile(file));
            statement.setString(2, "Migration " + file.getName());
            statement.setBoolean(3, true);
            statement.executeUpdate();
        }
    }
}

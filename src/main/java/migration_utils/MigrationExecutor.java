package migration_utils;

import database.ConnectionManager;

import java.sql.*;
import java.util.concurrent.TimeUnit;

public class MigrationExecutor {

    private static final int MAX_RETRIES = 3;

    MigrationExecutor(){
    }

    public static void execute(String sql) throws SQLException {
        int attempts = 0;
        while (attempts < MAX_RETRIES) {
            try (Connection connection = ConnectionManager.getConnection()) {
                connection.setAutoCommit(false);

                try (Statement statement = connection.createStatement()) {
                    statement.executeUpdate(sql);
                    connection.commit();
                    return;
                } catch (SQLTransientConnectionException e) {
                    attempts++;
                    System.out.println("Ошибка подключения к базе данных. Попытка №" + attempts);
                    if (attempts < MAX_RETRIES) {
                        waitBeforeRetry(attempts);
                    } else {
                        System.out.println("Превышено максимальное количество попыток подключения. Остановка.");
                        throw new SQLException("Ошибка при подключении к базе данных", e);
                    }
                } catch (SQLNonTransientConnectionException e) {
                    System.out.println("Ошибка подключения к базе данных. Не удается восстановить соединение.");
                    throw new SQLException("Невозможно восстановить соединение с базой данных", e);
                } catch (SQLException e) {
                    connection.rollback();
                    System.out.println("Ошибка при выполнении миграции. Транзакция откатана.");
                    throw new RuntimeException("Ошибка миграции, изменения откатаны", e);
                }
            } catch (SQLException e) {
                System.out.println("Ошибка при подключении к базе данных: " + e.getMessage());
                throw new SQLException("Ошибка при подключении к базе данных", e);
            }
        }
    }

    private static void waitBeforeRetry(int attempt) {
        try {
            long waitTime = (long) Math.pow(2, attempt) * 1000;
            System.out.println("Задержка перед повторной попыткой: " + waitTime / 1000 + " секунд.");
            TimeUnit.MILLISECONDS.sleep(waitTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Ошибка при ожидании между попытками: " + e.getMessage());
        }
    }
}

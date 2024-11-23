package database;

import lombok.extern.slf4j.Slf4j;
import utils.PropertiesUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Slf4j
public class ConnectionManager {

    private ConnectionManager() {
    }

    public static Connection getConnection() throws SQLException {
        String url = PropertiesUtils.getProperty("db.url");
        String username = PropertiesUtils.getProperty("db.username", "root");
        String password = PropertiesUtils.getProperty("db.password", "root");

        try {
            log.debug("Подключение к базе данных...");
            Connection connection = DriverManager.getConnection(url, username, password);
            if (connection == null || !connection.isValid(5)) {
                throw new SQLException("Ошибка установления соединения с базой данных. URL: " + url);
            }
            log.info("Соединение установлено.");
            return connection;
        } catch (SQLException e) {
            log.error("Ошибка подключения к базе данных: URL={}; user={}", url, username, e);
            throw e;
        }
    }
}


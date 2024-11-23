package database;

import utils.PropertiesUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionManager {

    private static Connection connection;

    private ConnectionManager() {
    }

    public static Connection getConnection() throws SQLException {
        String url = PropertiesUtils.getProperty("db.url");
        String user = PropertiesUtils.getProperty("db.username");
        String password = PropertiesUtils.getProperty("db.password");
        return DriverManager.getConnection(url, user, password);
    }
}

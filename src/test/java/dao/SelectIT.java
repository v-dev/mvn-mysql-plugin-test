package dao;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import static org.fest.assertions.api.Assertions.assertThat;

public class SelectIT {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private DataSource dataSource;
    private Connection connection;

    private final String USER = "user";

    private void setupDataSource() {
        dataSource = new MysqlDataSource();
        ((MysqlDataSource) dataSource).setDatabaseName("testdb");
        ((MysqlDataSource) dataSource).setPort(43444);
        ((MysqlDataSource) dataSource).setUser(USER);
        ((MysqlDataSource) dataSource).setPassword("password");
        ((MysqlDataSource) dataSource).setServerName("localhost");
    }

    @Test
    public void testSysdateIsToday() {
        setupDataSource();
        newConnection();

        Date actual = selectSysdate();
        logger.info("sysdate: " + actual);

        final Date today = new Date();
        assertThat(actual).isInSameDayAs(today);
    }

    @Test
    public void isCustomUser() {
        setupDataSource();
        newConnection();

        String actual = selectUser();
        logger.info("actual: " + actual);

        assertThat(actual).startsWith(USER);
    }

    @After
    public void teardown() {
        closeConnection();
    }

    private void newConnection() {
        try {
            logger.info("Trying newConnection...");
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            logger.error("newConnection(): Error getting Connection from DataSource: ", e);
        }
    }

    private void closeConnection() {
        try {
            if (!connection.isClosed()) {
                logger.info("Trying to closeConnection...");
                connection.close();
            }
        } catch (SQLException e) {
            logger.error("closeConnection(): Problem closing connection: ", e);
        }
    }

    private Date selectSysdate() {
        return executeSimpleSelect("SELECT SYSDATE()", Date.class);
    }

    private String selectUser() {
        return executeSimpleSelect("SELECT USER()", String.class);
    }

    private <T> T executeSimpleSelect(String selectsql, Class<T> returnType) {
        T result = null;

        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(selectsql);
        } catch (SQLException e) {
            logger.error("Error preparing statement: ", e);
        }

        ResultSet resultset = null;
        try {
            resultset = statement.executeQuery();
        } catch (SQLException e) {
            logger.error("Error executing select: ", e);
        }

        try {
            while (resultset.next()) {
                result = (T) resultset.getObject(1);
                logger.info("Got the following data: " + result);
            }
        } catch (SQLException e) {
            logger.error("Another SQLException, this time during reading resultset: ", e);
        } finally {
            try {
                resultset.close();
                statement.close();
                closeConnection();
            } catch (SQLException e) {
                logger.error("Error trying to close: ", e);
            }
        }

        return result;
    }

}

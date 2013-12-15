package dao;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;

public class SelectSysdateIT {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private DataSource dataSource;
    private Connection connection;

    private void setupDataSource() {
        dataSource = new MysqlDataSource();
        ((MysqlDataSource) dataSource).setDatabaseName("root");
        ((MysqlDataSource) dataSource).setPort(43444);
        ((MysqlDataSource) dataSource).setUser("root");
        ((MysqlDataSource) dataSource).setPassword("root");
        ((MysqlDataSource) dataSource).setServerName("localhost");
    }

    @Test
    public void test() {
        setupDataSource();
        newConnection();

        String actual = selectSysdate();
        logger.info("sysdate: " + actual);
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
            logger.error("newConnection(): Error getting Connection from DataSource: " + e);
        }
    }

    private void closeConnection() {
        try {
            if (!connection.isClosed()) {
                logger.info("Trying to closeConnection...");
                connection.close();
            }
        } catch (SQLException e) {
            logger.error("closeConnection(): Problem closing connection: " + e );
        }
    }

    private String selectSysdate() {
        String selectSql = "SELECT SYSDATE()";
        Date date = null;

        PreparedStatement statement = null;
        try {
            statement = connection.prepareStatement(selectSql);
        } catch (SQLException e) {
            logger.error("error preparing statement: " + e);
        }

        ResultSet resultset = null;
        try {
            resultset = statement.executeQuery();
        } catch (SQLException e) {
            logger.error("error executing select: " + e);
        }

        try {
            while ( resultset.next() ) {
                date = resultset.getDate(1);
                logger.info("Got date: " + date);
            }
        } catch (SQLException e) {
            logger.error("OMG, another sqlexception, this time during reading resultset: " + e);
        } finally {
            try {
                resultset.close();
                statement.close();
                closeConnection();
            } catch (SQLException e) {
                logger.error("Can't get anything right!  Error trying to close!: " + e);
            }
        }

        return date.toString();
    }

}

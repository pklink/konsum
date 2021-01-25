package net.einself.konsum;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.Consumer;

public class Application {

    private final static Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        final var dataSource = getDataSource();
        final var konsum = new DatabaseKonsumImpl(dataSource);

        konsum.connection(Application::createPersonTable);
        konsum.write(Application::createPerson);
        konsum.write(Application::createPerson);
        konsum.write(Application::createPerson);
        konsum.read(Application::listPersons);
    }

    private static Consumer<Statement> getPerson(int id) {
        final var vulnQuery = String.format("SELECT * FROM person WHERE id=%d", id);
        return statement -> {
            try {
                final var resultSet = statement.executeQuery(vulnQuery);
                outputPersons(resultSet);
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        };
    }

    private static void outputPersons(ResultSet resultSet) {
        try {
            while (resultSet.next()) {
                final var id = resultSet.getInt("id");
                final var name = resultSet.getString("name");
                LOGGER.info("Person(id={}, name={})", id, name);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private static void listPersons(Statement statement) {
        try {
            final var resultSet = statement.executeQuery("SELECT * FROM person");
            outputPersons(resultSet);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private static void createPersonTable(java.sql.Connection connection) {
        try {
            connection.createStatement().execute("CREATE TABLE `person` (\n" +
                "  `id` int(11) NOT NULL AUTO_INCREMENT,\n" +
                "  `name` varchar(255) DEFAULT NULL,\n" +
                "  PRIMARY KEY (`id`)\n" +
                ")");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    private static void createPerson(Statement statement) {
        try {
            statement.execute("INSERT INTO person (name) VALUES('Max Mustermann')");
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    private static DataSource getDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:h2:mem:konsum");
        return new HikariDataSource(config);
    }

}

package net.einself.konsum;

import java.sql.Connection;
import java.sql.Statement;
import java.util.function.Consumer;

public interface DatabaseKonsum {

    void connection(Consumer<Connection> consumer);

    void read(Consumer<Statement> consumer);

    void write(Consumer<Statement> consumer);

}

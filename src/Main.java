import org.h2.jdbcx.JdbcConnectionPool;

import java.time.LocalDate;

public class Main {

    public static void main(String[] args) {
        JdbcConnectionPool pool = JdbcConnectionPool.create("jdbc:h2:./db;MODE=MySQL", "", "");
        pool.setMaxConnections(25);
        Ranking ranking = new Ranking(LocalDate.now().minusWeeks(1), pool);
        ranking.generateRanking();
        ranking.printTable();
        pool.dispose();
    }

}

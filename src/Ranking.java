import org.h2.jdbcx.JdbcConnectionPool;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.Year;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Ranking {

    private final int endWeek;
    private final Year endYear;
    private final JdbcConnectionPool connectionPool;
    private final int[][] weightIndex;

    /**
     * Creates a ranking ending at the sunday of the current week, i.e. already after the events of the current week.
     *
     * @param connectionPool the Connection pool that should be used to connect to the database.
     */
    public Ranking(JdbcConnectionPool connectionPool) {
        this(LocalDate.now().with(TemporalAdjusters.nextOrSame(DayOfWeek.SUNDAY)).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR), Year.now(), connectionPool);
    }

    /**
     * Creates a ranking ending at given week in the given year, i.e. already after the events of that week.
     *
     * @param endWeek        the week the ranking should end at.
     * @param endYear        the year the ranking should end at.
     * @param connectionPool the Connection pool that should be used to connect to the database.
     */
    public Ranking(int endWeek, Year endYear, JdbcConnectionPool connectionPool) {
        this.endWeek = endWeek;
        this.endYear = endYear;
        this.connectionPool = connectionPool;
        this.weightIndex = getWeightIndex();
    }

    /**
     * Creates a ranking ending at given date, i.e. already after the events of that week.
     *
     * @param endDate        the date the ranking should end at.
     * @param connectionPool the Connection pool that should be used to connect to the database.
     */
    public Ranking(LocalDate endDate, JdbcConnectionPool connectionPool) {
        this(endDate.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR), Year.from(endDate), connectionPool);
    }

    /**
     * Generates the Ranking associated with the week and year of this Ranking and stores it. Updates and uses the data from the database.
     */
    public void generateRanking() {
        //Create Tables (only needed once) (Assuming if the file is there the tables have been created.)
        if(!Files.exists(Path.of("db.mv.db"))) {
            try(var conn = connectionPool.getConnection()) {
                conn.createStatement().execute("CREATE TABLE EVENT (ID INTEGER NOT NULL, NAME TEXT, WEEK TINYINT, YEAR YEAR, CONSTRAINT EVENT_PK PRIMARY KEY (ID));\n" +
                        "CREATE TABLE PLAYERS (ID INTEGER NOT NULL, NAME TEXT, CONSTRAINT PLAYERS_PK PRIMARY KEY (ID));\n" +
                        "CREATE TABLE POINTS (EVENT_ID  INTEGER NOT NULL, PLAYER_ID INTEGER NOT NULL, POINTS BIGINT NOT NULL, CONSTRAINT POINTS_EVENT_ID_FK FOREIGN KEY " +
                        "(EVENT_ID) REFERENCES EVENT(ID), CONSTRAINT POINTS_PLAYERS_ID_FK FOREIGN KEY (PLAYER_ID) REFERENCES PLAYERS(ID));\n" +
                        "CREATE TABLE WEIGHTED_POINTS(PLAYER_ID INTEGER NOT NULL, COUNT INTEGER, WEIGHTED_POINTS BIGINT, WEEK TINYINT NOT NULL, YEAR YEAR NOT NULL, CONSTRAINT " +
                        "WEIGHTED_POINTS_PK PRIMARY KEY (PLAYER_ID, WEEK, YEAR), CONSTRAINT WEIGHTED_POINTS_PLAYERS_ID_FK FOREIGN KEY (PLAYER_ID) REFERENCES PLAYERS (ID));");
            } catch(SQLException e) {
                System.err.println("Some error occurred when creating the database.");
                e.printStackTrace();
            }
        }

        List<Event> events = new ArrayList<>();
        try(var connection = connectionPool.getConnection()) {
            connection.createStatement().execute("TRUNCATE TABLE WEIGHTED_POINTS;");
            var eventInsertion = connection.prepareStatement("INSERT IGNORE INTO EVENT VALUES (?,?,?,?)");

            events = eventsForYear(this.endYear, eventInsertion);
            var events1 = eventsForYear(this.endYear.minusYears(1), eventInsertion);
            var events2 = eventsForYear(this.endYear.minusYears(2), eventInsertion);
            events.addAll(events1);
            events.addAll(events2);

        } catch(SQLException e) {
            System.err.println("Some error occurred while interacting with the database. Most likely when opening/closing the connection or creating the statements.");
            e.printStackTrace();
        }
        analyzeEvents(events);
        reevalutePlayersOver52Events();

    }

    /**
     * Analyzes the events in the given List.
     * <p>
     * Adds the results into the database. Uses all available processors.
     *
     * @param events the events to analyze.
     */
    private void analyzeEvents(List<Event> events) {
        List<Future<?>> futures = new ArrayList<>();
        ExecutorService executorService = Executors.newWorkStealingPool();
        int taskCount = 10;
        for(int task = 0; task < taskCount; task++) {
            int finalTask = task;
            Future<?> submit = executorService.submit(() -> {
                try(var connection = connectionPool.getConnection()) {
                    var playerInsertion = connection.prepareStatement("INSERT IGNORE INTO PLAYERS VALUES (?,?)");
                    var pointsInsertion = connection.prepareStatement("INSERT IGNORE INTO POINTS VALUES (?,?,?)");
                    var weightedPointsInsertion = connection.prepareStatement("INSERT INTO WEIGHTED_POINTS VALUES (?, 1, ?, " + this.endWeek + " ," + this.endYear.getValue() +
                            ") ON DUPLICATE KEY UPDATE COUNT = COUNT + 1,  WEIGHTED_POINTS = WEIGHTED_POINTS + VALUES(WEIGHTED_POINTS)");

                    var pointSelection = connection.prepareStatement("SELECT * FROM POINTS WHERE EVENT_ID = ?");

                    for(int i = finalTask; i < events.size(); i += taskCount) {
                        Event event = events.get(i);
                        event.analyze(weightIndex[event.yearNum][event.week - 1], pointSelection, playerInsertion, pointsInsertion, weightedPointsInsertion);
                    }
                } catch(SQLException e) {
                    System.err.println("Some error occurred when opening the connection/creating the statements.");
                    e.printStackTrace();
                }
            });
            futures.add(submit);
        }
        executorService.shutdown();
        futures.forEach(future -> {
            try {
                future.get();
            } catch(InterruptedException | ExecutionException e) {
                System.err.println("Could not complete waiting for some future.");
                e.printStackTrace();
            }
        });
    }

    /**
     * For players with more than 52 Events only the 52 most recent events count.
     * <p>
     * So for those players we reset the weighted points to 0 and recalculate them with only the most recent 52 events they played.
     */
    private void reevalutePlayersOver52Events() {
        try(var connection = connectionPool.getConnection()) {
            connection.createStatement().execute("UPDATE WEIGHTED_POINTS SET WEIGHTED_POINTS = 0 WHERE COUNT > 52;");
            var statement = connection.createStatement();
            var resultSet = statement.executeQuery("SELECT EVENT_ID, PLAYER_ID, POINTS, WEEK, YEAR FROM (SELECT EVENT_ID, EVENT.WEEK, EVENT.YEAR, POINTS, p.PLAYER_ID, " +
                    "row_number() OVER (PARTITION BY p.PLAYER_ID ORDER BY p.PLAYER_ID, EVENT.YEAR DESC, EVENT.WEEK DESC) \"row\" FROM POINTS p INNER JOIN EVENT ON EVENT_ID = ID " +
                    "INNER JOIN WEIGHTED_POINTS on p.PLAYER_ID = WEIGHTED_POINTS.PLAYER_ID WHERE COUNT > 52 AND WEIGHTED_POINTS.WEEK = " + this.endWeek + " AND WEIGHTED_POINTS" +
                    ".YEAR = " + this.endYear + ") tbl WHERE \"row\" <= 52;");
            var weightedPointsInsertion = connection.prepareStatement("INSERT INTO WEIGHTED_POINTS VALUES (?, 1, ?, " + this.endWeek + " ," + this.endYear.getValue() + ")\n" +
                    "ON DUPLICATE KEY UPDATE WEIGHTED_POINTS = WEIGHTED_POINTS + VALUES(WEIGHTED_POINTS)");

            while(resultSet.next()) {
                int playerID = resultSet.getInt("PLAYER_ID");
                //These points are multiplied by 100
                long unweightedPoints = resultSet.getLong("POINTS");
                var yearNum = 2 - (endYear.getValue() - resultSet.getInt("YEAR"));
                var week = resultSet.getInt("WEEK");
                //The points are now multiplied by 100 * 10,000 = 1,000,000
                var weight = this.weightIndex[yearNum][week - 1];
                long weightedPoints = unweightedPoints * weight;
                //The points are now multiplied by 100
                weightedPointsInsertion.setInt(1, playerID);
                weightedPointsInsertion.setLong(2, weightedPoints);
                weightedPointsInsertion.execute();
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Gets the event list for the given year of owgr.com and parses it into {@link Event}s.
     * <p>
     * Returns a List containing the events. Inserts the events into the database.
     *
     * @param year           the year to get the events for.
     * @param eventInsertion the Statement used to insert the events (1st parameter is the eventID, 2nd the name, 3rd the week, 4th the year)
     *
     * @return a List containing the events.
     */
    private List<Event> eventsForYear(Year year, PreparedStatement eventInsertion) {
        List<Event> eventList = new ArrayList<>();
        Document parse;
        try {
            var url = new URL("http://www.owgr.com/events?pageNo=1&pageSize=ALL&tour=&year=" + year);
            parse = Jsoup.parse(url, 10000);
        } catch(IOException e) {
            System.err.println("Failed to parse the events for year " + year + " with the following exception");
            throw new UncheckedIOException(e);
        }
        var select = parse.select("#ctl1 > tbody > tr");
        for(var element : select) {
            var week = element.getElementById("ctl2");
            var year1 = element.getElementById("ctl3");
            var event = element.getElementById("ctl5");
            var event1 = new Event(week, year1, event, this.endYear);
            eventList.add(event1);

            insertEventIntoDatabase(event1, eventInsertion);
        }
        return eventList;
    }

    /**
     * Inserts the given event into the database.
     *
     * @param event          the event to insert.
     * @param eventInsertion the Statement used to insert the events (1st parameter is the eventID, 2nd the name, 3rd the week, 4th the year)
     */
    private void insertEventIntoDatabase(Event event, PreparedStatement eventInsertion) {
        try {
            eventInsertion.setInt(1, event.id);
            eventInsertion.setString(2, event.name);
            eventInsertion.setInt(3, event.week);
            eventInsertion.setInt(4, event.year.getValue());
            eventInsertion.execute();
        } catch(SQLException e) {
            System.err.println("Could not insert the event " + event);
            e.printStackTrace();
        }
    }

    /**
     * Calculates the average points for each player, converts the numbers into decimal strings and prints a tab separated table into OWGR.txt and to System.out.
     */
    public void printTable() {
        PrintWriter writer;
        var averagePoints = new ArrayList<Long>();
        var names = new ArrayList<String>();

        try(var connection = connectionPool.getConnection()) {
            var statement = connection.createStatement();
            //Divides the points by eventCount or 40/52 if too small/large.
            //Also discards the last digit (points now multiplied by 100,000) because we only need 4 decimal places for displaying and one additional for rounding.
            var resultSet = statement.executeQuery("SELECT NAME, ((WEIGHTED_POINTS) / (LEAST(52, GREATEST(40, COUNT)) * 10)) AS AVERAGE_POINTS FROM WEIGHTED_POINTS, PLAYERS\n" +
                    "WHERE PLAYERS.ID = PLAYER_ID AND WEIGHTED_POINTS > 0 GROUP BY PLAYER_ID ORDER BY AVERAGE_POINTS DESC\n");
            while(resultSet.next()) {
                averagePoints.add(resultSet.getLong("AVERAGE_POINTS"));
                names.add(resultSet.getString("NAME"));
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }
        try {
            var fileWriter = new PrintWriter("OWGR.txt");
            OutputStream outputStream = new OutputStream() {
                @Override
                public void write(int i) {
                    fileWriter.write(i);
                    System.out.write(i);
                }

                @Override
                public void flush() {
                    fileWriter.flush();
                }

                @Override
                public void close() {
                    fileWriter.close();
                }
            };
            writer = new PrintWriter(outputStream, true);
        } catch(FileNotFoundException e) {
            System.err.println("Could not create fileWriter. Only printing to console.");
            e.printStackTrace();
            writer = new PrintWriter(System.out, true);
        }
        int place = 1;
        long previousPositionPoints = 0;
        int numberTied = 1;
        int maxNameLength = names.parallelStream().mapToInt(String::length).max().orElse(0);
        //At least 5 digits (pad with zeros)
        var format = new DecimalFormat("00000");
        for(int i = 0; i < averagePoints.size(); i++) {
            String playerName = names.get(i);
            //Round to 10s and discard last digit (now multiplied by 10,000)
            long roundedAverage = ((averagePoints.get(i) + 5) / 10);
            String averageAsString = format.format(roundedAverage);
            int endIndex = averageAsString.length() - 4;
            //Format it so the last 4 digits are after the decimal point.
            String formattedAverage = averageAsString.substring(0, endIndex) + "." + averageAsString.substring(endIndex);
            String placePlusTabs = place + "." + (place < 100 ? "\t\t" : "\t");
            String namePlusTabs = playerName + "\t".repeat(((maxNameLength - playerName.length() + 5) / 4));
            String output = placePlusTabs + namePlusTabs + (roundedAverage > 1000000 ? "" : " ") + formattedAverage;
            writer.println(output);
            if(previousPositionPoints == roundedAverage) {
                numberTied++;
            }
            else {
                place += numberTied;
                numberTied = 1;
            }
            previousPositionPoints = roundedAverage;
        }
        writer.close();
    }

    /**
     * Returns the weight index table ending at the current week number.
     * <p>
     * All decimal numbers are multiplied by 10000 to be able to be expressed as Integers.
     * <p>
     * The total weightIndex consists of 104 weeks. The firstIndex corresponds to the year (0-2), the second is the week in that year (0-51). The latest 13 weeks (up to endWeek in
     * year 2) have an index of 1.0000. Then after that every week before is 1/92 less worth. So "endWeek-14" is 91/92 rounded to 4 decimal places.
     *
     * @return the weight index table.
     */
    private int[][] getWeightIndex() {
        int[][] weightIndex = new int[3][52];
        int currentYear = 2;
        int currentWeek = this.endWeek - 1;
        //First 13 weeks full
        for(int i = 0; i < 13; i++) {
            weightIndex[currentYear][currentWeek] = 10000;
            currentWeek--;
            //Wrap to next year
            if(currentWeek < 0) {
                currentWeek = 51;
                currentYear--;
            }
        }
        var ninetyTwo = BigDecimal.valueOf(92);
        //Next 91 weeks 1/92 less on each one
        for(int i = 91; i >= 1; i--) {
            var tmp = BigDecimal.valueOf(i * 10000).divide(ninetyTwo, 0, RoundingMode.HALF_UP); //Result will have no decimal places
            weightIndex[currentYear][currentWeek] = tmp.intValue();
            currentWeek--;
            //Wrap to next year
            if(currentWeek < 0) {
                currentWeek = 51;
                currentYear--;
            }
        }
        return weightIndex;
    }
}

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
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

public class Ranking {

    private final int endWeek;
    private final Year endYear;
    private final int[][] weightIndex;
    private PreparedStatement eventInsertion;

    /**
     * Creates a ranking ending at the sunday of the current week, i.e. already after the events of the current week.
     */
    public Ranking() {
        this(LocalDate.now().with(TemporalAdjusters.nextOrSame(DayOfWeek.SUNDAY)).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR), Year.now());
    }

    /**
     * Creates a ranking ending at given week in the given year, i.e. already after the events of that week.
     *
     * @param endWeek the week the ranking should end at.
     * @param endYear the year the ranking should end at.
     */
    public Ranking(int endWeek, Year endYear) {
        this.endWeek = endWeek;
        this.endYear = endYear;
        this.weightIndex = getWeightIndex();
    }

    /**
     * Creates a ranking ending at given date, i.e. already after the events of that week.
     *
     * @param endDate the date the ranking should end at.
     */
    public Ranking(LocalDate endDate) {
        this(endDate.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR), Year.from(endDate));
    }

    /**
     * Generates the Ranking associated with the week and year of this Ranking and stores it. Updates and uses the data from the database.
     */
    public void generateRanking() {
        try {
            Class.forName("org.h2.Driver"); //Load right driver
        } catch(ClassNotFoundException e) {
            System.err.println("Couldn't find Database driver. Printing stacktrace and then exiting...");
            e.printStackTrace();
            return;
        }

        try(var connection = DriverManager.getConnection("jdbc:h2:./db;MODE=MySQL")) {
            //Create Tables (only needed once) (Assuming if the file is there the tables have been created.)
            if(!Files.exists(Path.of("db.mv.db"))) {
                connection.createStatement().execute("CREATE TABLE EVENT (ID INTEGER NOT NULL, NAME TEXT, WEEK TINYINT, YEAR YEAR, CONSTRAINT EVENT_PK PRIMARY KEY (ID));\n" +
                        "CREATE TABLE PLAYERS (ID INTEGER NOT NULL, NAME TEXT, CONSTRAINT PLAYERS_PK PRIMARY KEY (ID));\n" +
                        "CREATE TABLE POINTS (EVENT_ID  INTEGER NOT NULL, PLAYER_ID INTEGER NOT NULL, POINTS BIGINT NOT NULL, CONSTRAINT POINTS_EVENT_ID_FK FOREIGN KEY " +
                        "(EVENT_ID) REFERENCES EVENT(ID), CONSTRAINT POINTS_PLAYERS_ID_FK FOREIGN KEY (PLAYER_ID) REFERENCES PLAYERS(ID));\n" +
                        "CREATE TABLE WEIGHTED_POINTS(PLAYER_ID INTEGER NOT NULL, COUNT INTEGER, WEIGHTED_POINTS BIGINT, WEEK TINYINT NOT NULL, YEAR YEAR NOT NULL, CONSTRAINT " +
                        "WEIGHTED_POINTS_PK PRIMARY KEY (PLAYER_ID, WEEK, YEAR), CONSTRAINT WEIGHTED_POINTS_PLAYERS_ID_FK FOREIGN KEY (PLAYER_ID) REFERENCES PLAYERS (ID));");
            }

            connection.createStatement().execute("TRUNCATE TABLE WEIGHTED_POINTS;");
            eventInsertion = connection.prepareStatement("INSERT IGNORE INTO EVENT VALUES (?,?,?,?)");

            var events = eventsForYear(this.endYear);
            var events1 = eventsForYear(this.endYear.minusYears(1));
            var events2 = eventsForYear(this.endYear.minusYears(2));
            events.addAll(events1);
            events.addAll(events2);

            var playerInsertion = connection.prepareStatement("INSERT IGNORE INTO PLAYERS VALUES (?,?)");
            var pointsInsertion = connection.prepareStatement("INSERT IGNORE INTO POINTS VALUES (?,?,?)");
            var weightedPointsInsertion =
                    connection.prepareStatement("INSERT INTO WEIGHTED_POINTS VALUES (?, 1, ?, " + this.endWeek + " ," + this.endYear.getValue() + ")\n" +
                            "ON DUPLICATE KEY UPDATE COUNT = COUNT + 1,  WEIGHTED_POINTS = WEIGHTED_POINTS + VALUES(WEIGHTED_POINTS)");


            var pointSelection = connection.prepareStatement("SELECT * FROM POINTS WHERE EVENT_ID = ?");

            for(Event event : events) {
                event.analyze(weightIndex[event.yearNum][event.week - 1], pointSelection, playerInsertion, pointsInsertion, weightedPointsInsertion);
            }
        } catch(SQLException e) {
            System.err.println("Some error occurred while interacting with the database. Most likely when opening/closing the connection or creating the statements.");
            e.printStackTrace();
        }
    }

    /**
     * Gets the event list for the given year of owgr.com and parses it into {@link Event}s.
     * <p>
     * Returns a List containing the events. Inserts the events into the database.
     *
     * @param year the year to get the events for.
     *
     * @return a List containing the events.
     */
    private List<Event> eventsForYear(Year year) {
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

            insertEventIntoDatabase(event1);
        }
        return eventList;
    }

    /**
     * Inserts the given event into the database.
     *
     * @param event the event to insert.
     */
    private void insertEventIntoDatabase(Event event) {
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

        try(var connection = DriverManager.getConnection("jdbc:h2:./db;MODE=MySQL")) {
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
            String namePlusTabs = playerName + "\t".repeat(((maxNameLength - playerName.length() + 4) / 4));
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

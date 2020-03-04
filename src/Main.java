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
import java.util.*;
import java.util.stream.Collectors;

public class Main {

    private static final List<Event> eventList = new ArrayList<>();
    /**
     * Points are multiplied by 1,000,000.
     */
    private static final Map<Integer, Long> playerWeightedPointsMap = new HashMap<>();
    private static final Map<Integer, Integer> playerEventCountMap = new HashMap<>();
    private static final Map<Integer, String> playerNameMap = new HashMap<>();
    private static PreparedStatement eventInsertion;
    private static PreparedStatement playerInsertion;
    private static PreparedStatement pointsInsertion;
    private static PreparedStatement pointSelection;
    private static PreparedStatement playerSelection;

    public static void main(String[] args) {
        int endWeek = LocalDate.now().with(TemporalAdjusters.nextOrSame(DayOfWeek.SUNDAY)).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
        int[][] weightIndex = getWeightIndex(endWeek);
        Year endYear = Year.now();

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
                        "(EVENT_ID) REFERENCES EVENT(ID), CONSTRAINT POINTS_PLAYERS_ID_FK FOREIGN KEY (PLAYER_ID) REFERENCES PLAYERS(ID));");
            }

            eventInsertion = connection.prepareStatement("INSERT IGNORE INTO EVENT VALUES (?,?,?,?)");
            playerInsertion = connection.prepareStatement("INSERT IGNORE INTO PLAYERS VALUES (?,?)");
            pointsInsertion = connection.prepareStatement("INSERT IGNORE INTO POINTS VALUES (?,?,?)");

            pointSelection = connection.prepareStatement("SELECT * FROM POINTS WHERE EVENT_ID = ?");
            playerSelection = connection.prepareStatement("SELECT NAME FROM PLAYERS WHERE ID = ?");

            eventsForYear(endYear, endYear);
            eventsForYear(endYear.minusYears(1), endYear);
            eventsForYear(endYear.minusYears(2), endYear);

            for(Event event : eventList) {
                event.analyze(weightIndex[event.yearNum][event.week - 1], pointSelection, playerSelection, playerInsertion, pointsInsertion, playerWeightedPointsMap,
                        playerEventCountMap, playerNameMap);
            }

            printTable();

        } catch(SQLException e) {
            System.err.println("Some error occurred while interacting with the database. Most likely when opening/closing the connection or creating the statements.");
            e.printStackTrace();
        }
    }

    /**
     * Calculates the average points for each player, converts the numbers into decimal strings and prints a tab separated table into OWGR.txt and to System.out.
     */
    private static void printTable() {
        //Divides the points by eventCount or 40/52 if too small/large.
        //Also discards the last digit (points now multiplied by 100,000) because we only need 4 decimal places for displaying and one additional for rounding.
        var playerAveragePointMap = playerWeightedPointsMap.entrySet().parallelStream()
                .peek(entry -> entry.setValue(entry.getValue() / (Math.min(52, Math.max(40, playerEventCountMap.getOrDefault(entry.getKey(), 0))) * 10)))
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (o1, o2) -> o1, LinkedHashMap::new));
        PrintWriter writer;
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
        int maxNameLength = playerNameMap.entrySet().parallelStream().mapToInt(value -> value.getValue().length()).max().orElse(0);
        //At least 5 digits (pad with zeros)
        var format = new DecimalFormat("00000");
        for(Map.Entry<Integer, Long> playerIDPoints : playerAveragePointMap.entrySet()) {
            String playerName = playerNameMap.getOrDefault(playerIDPoints.getKey(), "Fail");
            //Round to 10s and discard last digit (now multiplied by 10,000)
            long roundedAverage = ((playerIDPoints.getValue() + 5) / 10);
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
     * Gets the event list for the given year of owgr.com and parses it into {@link Event}s.
     * <p>
     * Adds the event into the list. Also inserts them into the EVENT table of the database.
     *
     * @param year          the year to get the events for.
     * @param referenceYear the year that the period we are interested in ends at (to calculate the year index).
     */
    private static void eventsForYear(Year year, Year referenceYear) {
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
            var event1 = new Event(week, year1, event, referenceYear);
            eventList.add(event1);

            try {
                eventInsertion.setInt(1, event1.id);
                eventInsertion.setString(2, event1.name);
                eventInsertion.setInt(3, event1.week);
                eventInsertion.setInt(4, event1.year.getValue());
                eventInsertion.execute();
            } catch(SQLException e) {
                System.err.println("Could not insert the event " + event1);
                e.printStackTrace();
            }
        }
    }

    /**
     * Returns the weight index table ending at the given week number.
     * <p>
     * All decimal numbers are multiplied by 10000 to be able to be expressed as Integers.
     * <p>
     * The total weightIndex consists of 104 weeks. The firstIndex corresponds to the year (0-2), the second is the week in that year (0-51). The latest 13 weeks (up to endWeek in
     * year 2) have an index of 1.0000. Then after that every week before is 1/92 less worth. So "endWeek-14" is 91/92 rounded to 4 decimal places.
     *
     * @param endWeek the week the weight index ends on.
     *
     * @return the weight index table.
     */
    private static int[][] getWeightIndex(int endWeek) {
        int[][] weightIndex = new int[3][52];
        int currentYear = 2;
        int currentWeek = endWeek - 1;
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

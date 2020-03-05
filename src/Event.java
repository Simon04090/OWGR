import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Year;
import java.time.temporal.ChronoUnit;

/**
 * A class whose objects represent an OWGR event.
 */
public class Event {

    final int week;
    final Year year;
    final int yearNum;
    final int id;
    final String name;

    public Event(Element week, Element year, Element event, Year endYear) {
        this.week = week != null ? Integer.parseInt(week.html()) : -1;
        this.year = year != null ? Year.of(Integer.parseInt(year.html())) : Year.now();
        this.id = event != null ? Integer.parseInt(event.child(0).attr("href").split("=")[1]) : -1;
        this.yearNum = 2 - (int) this.year.until(endYear, ChronoUnit.YEARS);
        this.name = event != null ? event.child(0).html() : "";
    }

    /**
     * Calculates the index of the "Ranking Points" column on a parsed event result page from owgr.com.
     * <p>
     * It is necessary to calculate this dynamically because the events can have 3 or 4 rounds depending on the tour.
     *
     * @param parse the parsed event result page.
     *
     * @return the rankingPointsIndex.
     */
    private static int getPointPos(Document parse) {
        var header = parse.select("#phmaincontent_0_ctl00_PanelCurrentEvent > table > thead > tr:nth-child(2) > th");
        return header.indexOf(header.select(".ranking_points.ws.header").get(0)) + 1;

    }

    /**
     * Analyzes the event.
     * <p>
     * If the event was already parsed and is available in the database uses that data. Otherwise, parses the result page of this event on owgr.com. Parses the points of every
     * player, applies the given weight and adds it to the value saved in the WEIGHTED_POINTS table of the database and increases the count stored in that table. In case of parsing
     * it from the webpage also stores the playerName in the PLAYERS table and saves the unweighted points in the POINTS table.
     *
     * @param weight                  the weight to apply to the points.
     * @param pointSelection          the statement used to select the points (1st parameter is the eventID).
     * @param playerInsertion         the statement used to insert playerNames (1st parameter is the playerID, 2nd the playerName).
     * @param pointsInsertion         the statement used to insert unweightedPoints (1st parameter is the eventID, 2nd the playerID, 3rd unweightedPoints).
     * @param weightedPointsInsertion the statement used to insert weightedPoints (1st parameter is the playerID, 2nd weightedPoints).
     */
    public void analyze(int weight, PreparedStatement pointSelection, PreparedStatement playerInsertion, PreparedStatement pointsInsertion,
                        PreparedStatement weightedPointsInsertion) {
        if(weight != 0 && !tryToGetFromDatabase(weight, pointSelection, weightedPointsInsertion)) {
            Document parse = parseIntoDocument();

            int pointPos = getPointPos(parse);
            var players = parse.select("#phmaincontent_0_ctl00_PanelCurrentEvent > table > tbody > tr > td.name > a");
            var points = parse.select("#phmaincontent_0_ctl00_PanelCurrentEvent > table > tbody > tr > td:nth-child(" + pointPos + ")");

            if(players.size() != points.size()) {
                throw new IllegalStateException("Size of player list does not equal size of point list for event " + this.id);
            }

            for(int i = 0; i < players.size(); i++) {
                int playerID = Integer.parseInt(players.get(i).attr("href").split("=")[1]);
                //The following is done with Strings to avoid rounding errors.
                //Add ".00", so if the number wasn't decimal we get one with two decimal places.
                String[] splitByDecimalPoint = (points.get(i).html() + ".00").split("\\.");
                //Only keep two decimal places
                if(splitByDecimalPoint[1].length() > 2) {
                    splitByDecimalPoint[1] = splitByDecimalPoint[1].substring(0, 2);
                }
                //String of the points as whole number multiplied by 100 rounded down
                String unweightedString = splitByDecimalPoint[0] + splitByDecimalPoint[1];
                //If we only had one decimal place "multiply" by 10 because it was only 10 times the actual value
                if(splitByDecimalPoint[1].length() < 2) {
                    unweightedString += "0";
                }
                long unweightedPoints = Long.parseLong(unweightedString);
                //The points are now multiplied by 100 * 10,000 = 1,000,000
                long weightedPoints = unweightedPoints * weight;
                String playerName = players.get(i).html();

                try {
                    playerInsertion.setInt(1, playerID);
                    playerInsertion.setString(2, playerName);
                    playerInsertion.execute();
                    weightedPointsInsertion.setInt(1, playerID);
                    weightedPointsInsertion.setLong(2, weightedPoints);
                    weightedPointsInsertion.execute();
                    pointsInsertion.setInt(1, this.id);
                    pointsInsertion.setInt(2, playerID);
                    pointsInsertion.setLong(3, unweightedPoints);
                    pointsInsertion.execute();
                } catch(SQLException e) {
                    System.err.println("Could not insert the points/name of (" + playerID + ", " + playerName + ") for event " + this);
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Parses this event from the page on owgr.com
     *
     * @return the parsed event page.
     *
     * @throws UncheckedIOException when there is an IOException when trying to parse the event page.
     */
    private Document parseIntoDocument() {
        Document parse;
        try {
            parse = Jsoup.parse(new URL("http://www.owgr.com/en/Events/EventResult.aspx?eventid=" + this.id), 10000);
        } catch(IOException e) {
            System.err.println("Failed to parse the results for the event " + this + " with the following exception");
            throw new UncheckedIOException(e);
        }
        return parse;
    }

    /**
     * Tries to extract the points for this event out of the database.
     * <p>
     * Gets the points of every player, applies the given weight and adds it to the value saved in the WEIGHTED_POINTS table of the database and increases the count stored in that
     * table.
     *
     * @param weight                  the weight to apply to the points.
     * @param pointSelection          the statement used to select the points (1st parameter is the eventID).
     * @param weightedPointsInsertion the statement used to insert weightedPoints (1st parameter is the playerID, 2nd weightedPoints).
     *
     * @return true if the data could be gotten from the database, false otherwise.
     */
    private boolean tryToGetFromDatabase(int weight, PreparedStatement pointSelection, PreparedStatement weightedPointsInsertion) {
        try {
            pointSelection.setInt(1, this.id);
            ResultSet resultSet = pointSelection.executeQuery();
            if(resultSet.next()) {
                do {
                    int playerID = resultSet.getInt("PLAYER_ID");
                    //These points are multiplied by 100
                    long unweightedPoints = resultSet.getLong("POINTS");
                    //The points are now multiplied by 100 * 10,000 = 1,000,000
                    long weightedPoints = unweightedPoints * weight;
                    weightedPointsInsertion.setInt(1, playerID);
                    weightedPointsInsertion.setLong(2, weightedPoints);
                    weightedPointsInsertion.execute();

                } while(resultSet.next());
                return true;
            }
        } catch(SQLException e) {
            System.err.println("The following error occurred when trying to get the information for the event " + this + " out of the database. Trying to get it normally.");
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public String toString() {
        return "Event{" +
                "week=" + week +
                ", year=" + year +
                ", id=" + id +
                '}';
    }
}

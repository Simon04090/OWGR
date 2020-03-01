import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
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

    public static void main(String[] args) throws IOException {
        int endWeek = LocalDate.now().with(TemporalAdjusters.nextOrSame(DayOfWeek.SUNDAY)).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
        int[][] weightIndex = getWeightIndex(endWeek);
        Year endYear = Year.now();
        eventsForYear(endYear, endYear);
        eventsForYear(endYear.minusYears(1), endYear);
        eventsForYear(endYear.minusYears(2), endYear);

        for(Event event : eventList) {
            analyzeEvent(event, weightIndex[event.yearNum][event.week - 1]);
        }

        printTable();
    }

    /**
     * Analyzes the event.
     * <p>
     * Parses the result page of the given event.Parses the points of every player, applies the given weight and saves it in the playerWeightedPointsMap, also increases the count
     * stored in the playerEventCountMap and stores the playerName in the playerNameMap.
     *
     * @param event  the event to analyze.
     * @param weight the weight
     *
     * @throws IOException if HtmlCleaner throws one when accessing the URL.
     */
    private static void analyzeEvent(Event event, int weight) throws IOException {
        if(weight != 0) {
            var parse = Jsoup.parse(new URL("http://www.owgr.com/en/Events/EventResult.aspx?eventid=" + event.id), 10000);

            int pointPos = getPointPos(parse);
            var players = parse.select("#phmaincontent_0_ctl00_PanelCurrentEvent > table > tbody > tr > td.name > a");
            var points = parse.select("#phmaincontent_0_ctl00_PanelCurrentEvent > table > tbody > tr > td:nth-child(" + pointPos + ")");

            if(players.size() != points.size()) {
                throw new IllegalStateException("Size of player list does not equal size of point list for event " + event.id);
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
                if(weightedPoints != 0) {
                    playerWeightedPointsMap.put(playerID, playerWeightedPointsMap.getOrDefault(playerID, 0L) + weightedPoints);
                }
                playerEventCountMap.put(playerID, playerEventCountMap.getOrDefault(playerID, 0) + 1);
                String playerName = players.get(i).html();
                playerNameMap.put(playerID, playerName);
            }
        }
    }

    /**
     * Calculates the index of the "Name" and "Ranking Points" columns on a parsed event result page from owgr.com.
     * <p>
     * It is necessary to calculate this dynamically because the events can have 3 or 4 rounds depending on the tour.
     *
     * @param parse the parsed event result page.
     *
     * @return an array of the form [nameIndex, rankingPointsIndex].
     */
    private static int getPointPos(Document parse) {
        var header = parse.select("#phmaincontent_0_ctl00_PanelCurrentEvent > table > thead > tr:nth-child(2) > th");
        return header.indexOf(header.select(".ranking_points.ws.header").get(0)) + 1;

    }

    /**
     * Calculates the average points for each player, converts the numbers into decimal strings and prints a tab separated table into OWGR.txt and to System.out.
     *
     * @throws FileNotFoundException when the output file could not be created/edited.
     */
    private static void printTable() throws FileNotFoundException {
        //Divides the points by eventCount or 40/52 if too small/large.
        //Also discards the last digit (points now multiplied by 100,000) because we only need 4 decimal places for displaying and one additional for rounding.
        var playerAveragePointMap = playerWeightedPointsMap.entrySet().parallelStream()
                .peek(entry -> entry.setValue(entry.getValue() / (Math.min(52, Math.max(40, playerEventCountMap.getOrDefault(entry.getKey(), 0))) * 10)))
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (o1, o2) -> o1, LinkedHashMap::new));
        var fileWriter = new PrintWriter("OWGR.txt");
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
            fileWriter.println(output);
            System.out.println(output);
            if(previousPositionPoints == roundedAverage) {
                numberTied++;
            }
            else {
                place += numberTied;
                numberTied = 1;
                fileWriter.flush();
            }
            previousPositionPoints = roundedAverage;
        }
        fileWriter.close();
    }

    /**
     * Gets the event list for the given year of owgr.com and parses it into {@link Event}s.
     * <p>
     * Adds the event into the list.
     *
     * @param year          the year to get the events for.
     * @param referenceYear the year that the period we are interested in ends at (to calculate the year index).
     *
     * @throws IOException if HtmlCleaner throws one when accessing the URL.
     */
    private static void eventsForYear(Year year, Year referenceYear) throws IOException {
        var url = new URL("http://www.owgr.com/events?pageNo=1&pageSize=ALL&tour=&year=" + year);
        var parse = Jsoup.parse(url, 10000);
        var select = parse.select("#ctl1 > tbody > tr");
        for(var element : select) {
            var week = element.getElementById("ctl2");
            var year1 = element.getElementById("ctl3");
            var event = element.getElementById("ctl5");
            var event1 = new Event(week, year1, event, referenceYear);
            eventList.add(event1);
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

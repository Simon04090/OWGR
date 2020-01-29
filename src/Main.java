import org.htmlcleaner.*;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.filter.Filters;
import org.jdom2.input.SAXBuilder;
import org.jdom2.input.sax.BuilderErrorHandler;
import org.jdom2.input.sax.SAXEngine;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;
import org.xml.sax.SAXParseException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.net.URL;
import java.text.DecimalFormat;
import java.time.Year;
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
    private static final HtmlCleaner htmlCleaner = new HtmlCleaner();
    private static final JDomSerializer jDomSerializer = new JDomSerializer(new CleanerProperties(), true);
    private static SAXEngine saxEngine;

    public static void main(String[] args) throws IOException, JDOMException {
        int endWeek = 27;
        int[][] weightIndex = getWeightIndex(endWeek);
        Year endYear = Year.now();
        eventsForYear(endYear, endYear);
        eventsForYear(endYear.minusYears(1), endYear);
        eventsForYear(endYear.minusYears(2), endYear);

        SAXBuilder saxBuilder = new SAXBuilder();
        saxBuilder.setErrorHandler(new BuilderErrorHandler() {
            @Override
            public void error(SAXParseException exception) {
                exception.printStackTrace();
            }
        });
        saxEngine = saxBuilder.buildEngine();

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
    private static void analyzeEvent(Event event, int weight) throws IOException, JDOMException {
        if(weight != 0) {

            Document doc;
            String pathname = "cache\\" + event.id + ".xml";
            File file = new File(pathname);
            XmlSerializer xmlSerializer = new CompactXmlSerializer(new CleanerProperties());
            if(file.exists()) {
                doc = saxEngine.build(file);
            }
            else {
                URL url = new URL("http://www.owgr.com/en/Events/EventResult.aspx?eventid=" + event.id);
                TagNode clean = htmlCleaner.clean(url);
                doc = jDomSerializer.createJDom(clean);
                xmlSerializer.writeToFile(clean, pathname);
            }

            XPathFactory xPathFactory = XPathFactory.instance();

            int[] positions = getPositions(doc);

            XPathExpression<Element> exprPlayers = xPathFactory.compile("//div[2]/table/tbody/tr[td]/*[" + positions[0] + "]", Filters.element());
            List<Element> players = exprPlayers.evaluate(doc);

            XPathExpression<Element> exprPoints = xPathFactory.compile("//div[2]/table/tbody/tr[td]/*[" + positions[1] + "]", Filters.element());
            List<Element> points = exprPoints.evaluate(doc);

            if(players.size() != points.size()) {
                throw new IllegalStateException("Size of player list does not equal size of point list for event " + event.id);
            }
            for(int i = 0; i < players.size(); i++) {
                int playerID = Integer.parseInt(players.get(i).getChild("a").getAttributeValue("href").split("=")[1]);
                //The following is done with Strings to avoid rounding errors.
                //Add ".00", so if the number wasn't decimal we get one with two decimal places.
                String[] splitByDecimalPoint = (points.get(i).getValue() + ".00").split("\\.");
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
                String playerName = players.get(i).getChild("a").getValue();
                playerNameMap.put(playerID, playerName);
            }
        }
    }

    /**
     * Calculates the index of the "Name" and "Ranking Points" columns on a parsed event result page from owgr.com.
     * <p>
     * It is necessary to calculate this dynamically because the events can have 3 or 4 rounds depending on the tour.
     *
     * @param doc the parsed event result page.
     *
     * @return an array of the form [nameIndex, rankingPointsIndex].
     */
    private static int[] getPositions(Document doc) {
        XPathExpression<Element> expression = XPathFactory.instance().compile("//tr[2]/th", Filters.element());
        List<Element> evaluated = expression.evaluate(doc);
        int[] positions = {3, 9};
        for(int i = 0; i < evaluated.size(); i++) {
            Element element = evaluated.get(i);
            if(element.getValue().equals("Name")) {
                positions[0] = i + 1;
            }
            else if(element.getValue().equals("Ranking Points")) {
                positions[1] = i + 1;
            }
        }
        return positions;
    }

    /**
     * Calculates the average points for each player, converts the numbers into decimal strings and prints a tab separated table into OWGR.txt and to System.out.
     *
     * @throws FileNotFoundException when the output file could not be created/edited.
     */
    private static void printTable() throws FileNotFoundException {
        //Divides the points by eventCount or 40/52 if too small/large.
        //Also discards the last digit (points now multiplied by 100,000) because we only need 4 decimal places for displaying and one additional for rounding.
        LinkedHashMap<Integer, Long> playerAveragePointMap = playerWeightedPointsMap.entrySet().parallelStream()
                .peek(entry -> entry.setValue(entry.getValue() / (Math.min(52, Math.max(40, playerEventCountMap.getOrDefault(entry.getKey(), 0))) * 10)))
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (o1, o2) -> o1, LinkedHashMap::new));
        PrintWriter fileWriter = new PrintWriter("OWGR.txt");
        int i = 1;
        long previousPoints = 0;
        int same = 1;
        int maxNameLength = playerNameMap.entrySet().parallelStream().mapToInt(value -> value.getValue().length()).max().orElse(0);
        //At least 5 digits (pad with zeros)
        DecimalFormat format = new DecimalFormat("00000");
        for(Map.Entry<Integer, Long> entry : playerAveragePointMap.entrySet()) {
            Integer playerID = entry.getKey();
            Long averagePoints = entry.getValue();
            String name = playerNameMap.getOrDefault(playerID, "Fail");
            //Round to 10s and discard last digit (now multiplied by 10,000)
            long roundedAverage = ((averagePoints + 5) / 10);
            String averageAsString = format.format(roundedAverage);
            int endIndex = averageAsString.length() - 4;
            //Format it so the last 4 digits are after the decimal point.
            String formattedAverage = averageAsString.substring(0, endIndex) + "." + averageAsString.substring(endIndex);
            String placePlusTabs = i + "." + (i < 100 ? "\t\t" : "\t");
            String namePlusTabs = name + String.join("", Collections.nCopies(((maxNameLength - name.length() + 4) / 4), "\t"));
            String output = placePlusTabs + namePlusTabs + (roundedAverage > 1000000 ? "" : " ") + formattedAverage;
            fileWriter.println(output);
            System.out.println(output);
            if(previousPoints == roundedAverage) {
                same++;
            }
            else {
                i += same;
                same = 1;
                fileWriter.flush();
            }
            previousPoints = roundedAverage;
        }
        fileWriter.close();
    }

    /**
     * Gets the event list for the given year of owgr.com and parses it into {@link Event}s.
     * <p>
     * Adds the event into the list.
     *
     * @param year    the year to get the events for.
     * @param endYear the year that the period we are interested in ends at (to calculate the year index).
     *
     * @throws IOException if HtmlCleaner throws one when accessing the URL.
     */
    private static void eventsForYear(Year year, Year endYear) throws IOException {
        Document doc = jDomSerializer.createJDom(htmlCleaner.clean(new URL("http://www.owgr.com/events?pageNo=1&pageSize=ALL&tour=&year=" + year)));
        XPathFactory xPathFactory = XPathFactory.instance();
        XPathExpression<Element> expr = xPathFactory.compile("//div[3]/table/tbody/tr[td]", Filters.element());
        List<Element> stuff = expr.evaluate(doc);
        for(Element element : stuff) {
            Element week = null, year1 = null, event = null;
            for(Element child : element.getChildren()) {
                String id = child.getAttributeValue("id");
                if("ctl2".equals(id)) {
                    week = child;
                }
                else if("ctl3".equals(id)) {
                    year1 = child;
                }
                else if("ctl5".equals(id)) {
                    event = child;
                }

            }
            Event event1 = new Event(week, year1, event, endYear);
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
        BigDecimal ninetyTwo = BigDecimal.valueOf(92);
        //Next 91 weeks 1/92 less on each one
        for(int i = 91; i >= 1; i--) {
            BigDecimal tmp = BigDecimal.valueOf(i * 10000).divide(ninetyTwo, 0, BigDecimal.ROUND_HALF_UP); //Result will have no decimal places
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

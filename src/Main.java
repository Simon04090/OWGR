import org.htmlcleaner.CleanerProperties;
import org.htmlcleaner.HtmlCleaner;
import org.htmlcleaner.JDomSerializer;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.filter.Filters;
import org.jdom2.xpath.XPathExpression;
import org.jdom2.xpath.XPathFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.time.Year;
import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final List<Event> eventList = new ArrayList<>();

    public static void main(String[] args) throws IOException {
        int endWeek = 27;
        int[][] weightIndex = getWeightIndex(endWeek);
        Year endYear = Year.now();
        eventsForYear(endYear, endYear);
        eventsForYear(endYear.minusYears(1), endYear);
        eventsForYear(endYear.minusYears(2), endYear);
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
        Document doc = new JDomSerializer(new CleanerProperties(), true)
                .createJDom(new HtmlCleaner().clean(new URL("http://www.owgr.com/events?pageNo=1&pageSize=ALL&tour=&year=" + year)));
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

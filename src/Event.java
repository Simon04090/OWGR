import org.jdom2.Element;

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

    public Event(Element week, Element year, Element event, Year endYear) {
        this.week = week != null ? Integer.parseInt(week.getValue()) : -1;
        this.year = year != null ? Year.of(Integer.parseInt(year.getValue())) : Year.now();
        this.id = event != null ? Integer.parseInt(event.getChild("a").getAttribute("href").getValue().split("=")[1]) : -1;
        this.yearNum = 2 - (int) this.year.until(endYear, ChronoUnit.YEARS);
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

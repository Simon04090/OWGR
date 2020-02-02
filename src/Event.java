import org.jsoup.nodes.Element;

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
        this.week = week != null ? Integer.parseInt(week.html()) : -1;
        this.year = year != null ? Year.of(Integer.parseInt(year.html())) : Year.now();
        this.id = event != null ? Integer.parseInt(event.child(0).attr("href").split("=")[1]) : -1;
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

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.Year;
import java.time.temporal.IsoFields;
import java.time.temporal.TemporalAdjusters;

public class Main {

    public static void main(String[] args) {
        var endYear = Year.now();
        var endWeek = LocalDate.now().with(TemporalAdjusters.nextOrSame(DayOfWeek.SUNDAY)).get(IsoFields.WEEK_OF_WEEK_BASED_YEAR);
        Ranking ranking = new Ranking(endWeek, endYear);
        ranking.generateRanking();
        ranking.printTable();
    }

}

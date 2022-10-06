// Databricks notebook source
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class EventClassifier(label: String, likeQuery: List[String])

val CAPITAL_REGEX = "([0-9]+)?\\.?([0-9]+)?\\.?([0-9]+)\\,([0-9]+) (EUR|DM|DEM|GBP)"
val CAPITAL_REGEX_2 = "([0-9]+)?\\.?([0-9]+)?\\.?([0-9]+) (EUR|DM|DEM|GBP)"
val CAPITAL_REGEX_FRONT = "(EUR|DM|DEM|GBP) ([0-9]+)?\\.?([0-9]+)?\\.?([0-9]+)\\,([0-9]+)"
val CAPITAL_REGEX_FRONT_2 = "(EUR|DM|DEM|GBP) ([0-9]+)?\\.?([0-9]+)?\\.?([0-9]+)\\,([0-9]+)"


// events that are classified via publication body

// WATCH OUT FOR WILDCARDS AT END
// query like %%Geschäftsanschrift:%% runs EXTREMELY slow (never managed to finish it)
// seems like a  spark bug, todo file
val classifiableEvents = List(
  EventClassifier("officer_correction", List("Änderung zu Nr.")),
  EventClassifier("officers_removed", List("Nicht mehr %:", "Als nicht eingetragen wird veröffentlicht", "als nicht eingetragen wird veröffentlicht", "als nicht eingetragen wird bekanntgemacht")),
  EventClassifier("address", List("Geschäftsanschrift:", "Geschäftsanschrift jetzt:", 
    "Inländische Geschäftsanschrift lautet:", "Inländische Geschäftsanschrift;", "Die Geschäftsanschrift wurde wie folgt im Registerblatt vermerkt:", "Neue Anschrift:", 
    "Die neue Geschäftsanschrift lautet:", "Geschäftsanschrift geändert, nun:", "neue inländische Anschrift:", 
    "Geschäftsanschrift geändert, jetzt:", "Geschäftsanschrift neu:", "Geschäftsanschrift geändert, nunmehr:", 
    "Geschäftsanschrift nun:", "Geschäftsanschrift :", "Die Geschäftsanschrift ist geändert und somit wie folgt im Registerblatt vermerkt:", "Neu Geschäftsanschrift eigetragen:", 
    "Geschäftsanschrift;", "Geschäftsanschrift nunmehr:", "Anschrift jetzt:", "Neue inländische Anschrift:", "Geschäftsanschrift geändert, jetzt:", ", neue Anschrift")), 
EventClassifier("in_liquidation", List("Gesellschaft hat einen oder mehrere Liquidatoren", "alte Anschrift:", "bisherige Anschrift:")),
EventClassifier("liquidated", List("Die Liquidation ist beendet")),
EventClassifier("insolvency", List("Insolvenzverfahren")),
EventClassifier("branching_change", List("Zweigniederlassung", "Niederlassung")),
EventClassifier("branch_created", List("Zweigniederlassung%errichtet")),
EventClassifier("branch_ended", List("Zweigniederlassung%aufgehoben")),
EventClassifier("name_change", List("Der Name der Firma ist geändert in:", "Name der Firma:", "Der Name der Firma ist geändert", "Neue Firma:", "neue Firma:", "Firma lautet nunmehr:", "Firma von Amts wegen berichtigt in:", "Firma geändert; nun:", "Firma geändert:", "Firma geändert, nun:", "Die Firma lautet richtig", "Firma der Zweigniederlassung geändert; nun:", "Firma geändert in:", "Die Firma des Einzelunternehmens lautet:", "Die Firma ist geändert von vormals:", "Die Eintragung betreffend die Firma ist von Amts wegen berichtigt und wird wie folgt berichtigt eingetragen:", "Die Firma lautet jetzt:", "Firma lautet jetzt:", "Firma lautet richtig:", "Die Eintragung der Firma wird von Amts wegen wie folgt berichtigt:", "Die Firma ist geändert in:", "Firma geändert, nunmehr:", "Firma geändert; vormals:", "Firma geändert nun:", "Name der Firma ist geändert in:", "Die Firma ist geändert und lautet jetzt:", "Firma: %mbH", "Firma:% (haftungsbeschränkt)", "Firma:% AG", "Firma:% Aktiengesellschaft", "Firma von Amts wegen berichtigt, nun:", "Die Firma lautet nach Änderung nunmehr", "Die Firma lautet nach Änderung nun:", "Die Firma lautet nun:", "die Firma lautet nun:", "FIrma lautet nun:", "Firma lautet nun:", "Die Firma lautet nun", "Die Firma lautet jetzt", "lautet infolge Firmenändertung nunmehr:", "Die geänderte Firma lautet:", "Die Firma ist geändert und lautetjetzt:", "Die Firma lautet nunmehr", "Der neue Firmenname lautet nunmehr:", "geändert; sie lautet nun:", "geändert worden; sie lautet nun:", "die Firma lautet nach Änderung jetzt", "Die Firma lautet", "Die Firma ist geändert und lautetjetzt:", "Die Firma ist geändert unfd lautet jetzt:", "Die Firma lautet:", "Die Firma der Gesellschaft lautet richtig:", "die Firma lautet nach Änderung", "Die Eintragung betreffend die Firma wird berichtigt und wie folgt berichtigt eingetragen:", "Die Firma lautet richtig (v.A.w. berichtigt):", "die Firma ist geändert in: ", "die Firma lautete bisher:", "Die Firma lautete bisher:", "Firmenname geändert:", "Die Firma lautete zuvor:", "ist von Amts wegen berichtigt und wie folgt zur Firma ergänzt:;", "Firma berichtigt:;", "ist von Amts wegen berichtigt und wird wie folgt berichtigt eingetragen:;")), // TODO: sitz der firma problem
EventClassifier("capital_change", List("Stammkapital:", "Kapital:", "Grundkapital%erhöht", "Stammkapital%erhöht", "Kapital%berichtigt", "Erhöhung des Stammkapitals", "Stamm- bzw. Grundkapital:", "Grund- oder Stammkapital:", "Grundkapital nun:", "neues Stammkapital :", "Kapital geändert, nun:", "Grundkapital:", "Neues Stammkapital:", "Grundkapital jetzt:", "Stammkapital nun:", "Kapital nun:")),
EventClassifier("court_moved", List("Sitz%verlegt", "von%nach%verlegt", "neuer sitz", "Sitzverlegung")),
EventClassifier("court_reorg", List("Gesetzes zur Neuordnung")),
EventClassifier("objective_changed", List("Gegenstand:", "Gegenstand des Unternehmens:", "Unternehmensgegenstand:", "Gegenstand der Zweigniederlassung:", "Gegenstand geändert; nun:", "Unternehmensgegenstand ist:", "Gegenstand jetzt:", "Gegenstand lautet jetzt:", "Gegenstand von Amts wegen berichtigt, nun:", "Gegenstand des Unternehmens ist", "Gegenstand des Unternehmens sind", "Die Eintragung betreffend den Gegenstand ist von Amts wegen berichtigt und wird wie folgt berichtigt eingetragen:", "Gegenstand der Zweigniederlassung ist", "Gegenstand jetzt:", "Der Gegenstand lautet richtig:", "Gegenstandt:", "Gegenstand berichtigt:", "Neuer Unternehmensgegenstand:", "Neuer Gegenstand:", "Neuer Gegenstand des Unternehmens:", "neuer Gegenstand :")), //TODO handle: Zusätzlicher Unternehmensgegenstand:, Erweiterter Unternehmensgegenstand:, Ergänzter Unternehmensgegenstand:
EventClassifier("board_list_submitted", List("Die Liste der Aufsichtsratsmitglieder", "Die aktuelle Liste der Aufsichtsratsmitglieder%eingereicht")),
EventClassifier("legal_form_change", List("Rechtsverhaeltnis:", "Rechtsverhältnis:")),
//EventClassifier("legal_form_change", List("rechtsformwechsel")),
EventClassifier("company_agreement_changed", List("Gesellschaftsvertrag%geändert", "Gesellschaftsvertrag insgesamt neu gefasst", "Gesellschaftsvertrag neu gefasst", "Neufassung des Gesellschaftvertrags beschlossen", "Änderung des Gesellschaftsvetrages", "Änderung des Gesellschaftsvertrages")),
EventClassifier("gewinnabführungsvertrag", List("Gewinnabführungs")),
EventClassifier("ergebnisabführungsvertrag", List("Ergebnisabführungs")),
EventClassifier("controlling_agreement", List("Beherrschungs")),
EventClassifier("merger_agreement", List("Verschemlzungs")),
EventClassifier("judicial_deletion_announcement", List("Amts wegen zu löschen")),
EventClassifier("judicial_deletion", List("Amts wegen gelöscht")),
EventClassifier("judicial_deletion_cancelled", List("Amtslöschungsverfahren wird aufgehoben")),
EventClassifier("company_defunct", List("Die Firma ist erloschen")),
EventClassifier("bylaws_changed", List("Änderung der Satzung")),
EventClassifier("split", List("Spaltung")),
EventClassifier("replacement_company", List("Ersatzfirma"))
)

// check for double %, see below
for (ev <- classifiableEvents) {
  for (q <- ev.likeQuery) {
    assert(!q.startsWith("%"))
    assert(!q.endsWith("%"))
  }
}


val getEventQueryList = (eventName: String) =>  classifiableEvents.filter(e => e.label == eventName)(0).likeQuery


def getSentenceFilterQueryForEvent(eventName: String) = {
  val queryList = getEventQueryList(eventName)
  // filter(sentences, s -> ( ilike(s, '%gegenstand:%') | ilike(s, '%unternehmensgegenstand:%') | ilike(s, '%gegenstand des unternehmens:%') | ilike(s, '%gegenstand der zweigniederlassung:%') ))
  val queryString = queryList.map(likeQuery => s"ilike(s, '%$likeQuery%') ").mkString(" OR ")
  s"filter(sentences, s -> ( $queryString ))"
}

def getBlankReplacementRegexForEvent(eventName: String) =  {
  val likeQuery = getEventQueryList(eventName)
  val q = likeQuery.sortBy(-_.length).map(q => s"\\Q$q\\E").mkString("|")
  s".*?($q)"
}

// TODO custom udf that finds AND replaces in one step, also breaks when the target string was found
// should be much faster
// replace strings in column sequentially (usually from going from longest to shortest)
def replaceColSequentially(df:DataFrame, colName: String, eventName: String) = {
  val strings = getEventQueryList(eventName)
  strings.foldLeft(df)((df: DataFrame, s: String) => df.withColumn(colName, expr(s"split_part($colName, '$s', -1)")))
}

// COMMAND ----------

//# Double wildcard bug
//if using double wildcards it takes extremely long, todo file bug

// COMMAND ----------

// this works
//spark.sql("SELECT 1 FROM hreg_events_hrb_only WHERE publicationBody LIKE '%Geschäftsanschrift:%'").write.format("noop").mode("overwrite").save()

// COMMAND ----------

// this never finishes
//spark.sql("SELECT 1 FROM hreg_events_hrb_only WHERE publicationBody LIKE '%%Geschäftsanschrift:%%'").write.format("noop").mode("overwrite").save()
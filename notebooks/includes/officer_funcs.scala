// Databricks notebook source
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer


val titleTokenList = List(
    "Vorstand",
    "Geschäftsführer",
    "Geschäftsführerin",
    "Inhaber",
    "Inhaberin",
    "Partner",
    "Partnerin",
    "Prokura",
    "Einzelprokura",
    "Gesamtprokura",
    "Prokurist",
    "Partner",
    "Director",
    "director",
    "Persönlich haftender Gesellschafter",
    "Persönlich haftende Gesellschafterin",
    "direktor",
    "direktorin",
    "Geschäftsführer und ständiger Vertreter der Zweigniederlassung",
    "Ständiger Vertreter der Zweigniederlassung",
    "Ständiger Vertreter",
    "Liquidator",
    "Liquidatorin",
    "Verwaltungsrat",
    "Präsident",
    "Präsidentin",
    "Geschäftführerin",
    "Mitglied des Verwaltungsrates",
    "Mitglied des Aufsichtsrates",
    "Verwaltungsratsmitglied",
    "Geschäftsleiter",
    "Vorstandsvorsitzender",
    "Vorsitzende",
    "Vorstandsmitglied",
    "Vorstandsmitglied (Bestuurder)",
    "Geschäftsführer (director)",
    "Abwickler"
)

//legacy
val getTitleTokens = () => {
  val titleTokens = "Vorstand|Geschäftsführer|Geschäftsführerin|Inhaber|Inhaberin|Partner|Partnerin|Prokura|Einzelprokura|Gesamtprokura|Prokurist|Gesamtprokura gemeinsam mit einem (Geschäftsführer|Vorstandsmitglied|Verwaltungsratmitglied) oder einem anderen Prokuristen|(Einzelprokura|Gesamtprokura) mit der Ermächtigung zur Veräußerung und Belastung von Grundstücken|Einzelprokura mit der Befugnis, im Namen der Gesellschaft mit sich im eigenen Namen oder als Vertreter eines Dritten Rechtsgeschäfte abzuschließen|Partner|Director|director|Persönlich haftender Gesellschafter|Persönlich haftende Gesellschafterin|direktor|direktorin|Geschäftsführer und ständiger Vertreter der Zweigniederlassung|Ständiger Vertreter der Zweigniederlassung|Ständiger Vertreter|Liquidator|Liquidatorin|Verwaltungsrat|Präsident|Präsidentin|Geschäftführerin|Mitglied des (Verwaltungsrates|Aufsichtsrates)|Verwaltungsratsmitglied|Geschäftsleiter|Vorstandsvorsitzender|Vorsitzende|Vorstandsmitglied|Vorstandsmitglied \\(Bestuurder\\)|Geschäftsführer \\(director\\)|Geschäftsführender Direktor|Abwickler"
  titleTokens
} : String

//val splitSeperatedByUnd = (officerStr: String)


val splitOfficerStr = (officerStr: String, titleTokens: String) => {
  // workaround as these get split off a seperate item
  val prokuraStrings = List("Einzelprokura.", "Einzelprokura", "Prokura gemeinsam mit einem persönlich haftenden Gesellschafter oder einem weiteren Prokuristen", "Einzelprokura mit der Befugnis zur Veräußerung und Belastung von Grundstücken", "Prokura gemeinsam mit einem Geschäftsführer oder einem weiteren Prokuristen", "Einzelprokura mit der Befugnis Rechtsgeschäfte mit sich selbst oder als Vertreter Dritter abzuschließen mit der Befugnis zur Veräußerung und Belastung von Grundstücken", "Prokura gemeinsam mit einem Vorstand", "Prokura gemeinsam mit einem Geschäftsführer oder einem weiteren Prokuristen", "Prokura gemeinsam mit einem Geschäftsführer mit der Befugnis Rechtsgeschäfte mit sich selbst oder als Vertreter Dritter abzuschließen mit der Befugnis zur Veräußerung und Belastung von Grundstücken", "Prokura gemeinsam mit einem Geschäftsführer.", "mit der für sich sowie ihre Geschäftsführer geltenden Befugnis Rechtsgeschäfte mit sich selbst oder als Vertreter Dritter abzuschließen",  "Einzelprokura mit der Befugnis Rechtsgeschäfte mit sich selbst oder als Vertreter Dritter abzuschließen", "mit der Befugnis die Gesellschaft mit einem anderen Geschäftsführer oder einem Prokuristen zu vertreten", "Einzelprokura mit der Befugnis zur Veräußerung und Belastung von Grundstücken und grundstücksgleichen Rechten", "mit der für sich sowie ihre Geschäftsführer geltenden Befugnis Rechtsgeschäfte mit sich selbst oder als Vertreter Dritter abzuschließen", "mit der für ihre Geschäftsführer geltenden Befugnis, Rechtsgeschäfte mit sich selbst oder als Vertreter Dritter abzuschließen", "Prokura gemeinsam mit einem Geschäftsführer")
  // maybe merge these again?

  val exclP1 = s"(?<!($titleTokens)).*abzuschließen\\.?".r
  val exclP2 = s"(?<!($titleTokens)).*Grundstücken\\.?".r
  val exclP3 = s"(?<!($titleTokens)).*Prokuristen.*(?![\\*:])".r
  val exclP4 = s"(?<!($titleTokens)).*vertreten".r
  
  val STAR = "*".charAt(0)

  var officerStrings = officerStr
    .split("; mit der Befugnis(.*?)(abzuschließen|vertreten|Grundstücken)(;|\\.|$)")
    .flatMap(s => s.split("; Prokura gemeinsam mit einem Vorstand oder einem weiteren Prokuristen(;|\\.|$)?(?!:)"))
    .flatMap(s => s.split("; Einzelprokura mit der Befugnis(.*?)(abzuschließen|vertreten|Grundstücken)(;|\\.|$)?(?!:)"))
    .flatMap(s => s.split("(, vertretungsberechtigt gemeinsam mit einem anderen Geschäftsführer oder einem Prokuristen\\.|, vertretungsberechtigt gemeinsam mit einem weiteren Geschäftsführer\\.)"))
    .flatMap(s => s.split("; Einzelprokura(\\.|$|;)"))
    // Gesamtprokura gemeinsam mit dem Geschäftsführer Thomas Pscherer oder der Prokuristin Ulrike Sabel; mit der Befugnis, im Namen der Gesellschaft mit sich als Vertreter eines Dritten Rechtsgeschäfte abzuschließen mit der Ermächtigung zur Veräußerung und Belastung von Grundstücken: Künzel, Jan Rouven, Hamburg, *04.08.1973.
    .flatMap(s => s.trim.split("(; )?(jeweils )?mit der Befugnis(.*?)(abzuschließen|vertreten|Grundstücken)( und)?(?= Prokura:)")) //
    .flatMap(s => s.split("(?<=prokura|Prokura)(.*)mit der Befugnis,(.*?)(abzuschließen|vertreten|Grundstücken)(?=:)"))
    // inline gf berechtigung
    .flatMap(s => s.trim.split("(; )?(jeweils )?mit der Befugnis(.*?)(abzuschließen|vertreten|Grundstücken)( und)?(;|\\.|$)")) //
    .flatMap(s => s.split("((?<=\\*[0-9]{2}\\.[0-9]{2}\\.[0-9]{4})(\\.|;| und))")) // birthdate followed by dot eg "Blomberg, *02.06.1951. Nunmehr bestellt" OR birthdate followed by und "Blomberg, *02.06.1951 und Muster, Karl...."
    //.flatMap(s => s.split("(?<!\\.) ?(Prokura gemeinsam mit einem Geschäftsführer( oder einem weiteren Prokuristen)?)(?!:)")) 
    //.flatMap(s => s.split("(?<=Rechtsgeschäfte abzuschließen)(;|\\.)"))
    .flatMap(s => s.split("(?<=vertretungsberechtigt gemäß allgemeiner Vertretungsregelung)\\."))
    .flatMap(s => s.split("[\\.,] (?=Nicht |Ausgeschieden|War |Eingetreten|Prokura erloschen)")) //. Nicht mehr GF etc
    .flatMap(s => s.split("(?<!:);")) // semicolon except when preceeded by :
    .flatMap(s => s.split(",( jeweils)? einzelvertretungsberechtigt"))
    .flatMap(s => s.split(s"\\.\\s?(?=${titleTokens})"))
    //.flatMap(s => s.split(s"\\s?(?=(${titleTokens}))")) // check if this is needed
    .flatMap(s => s.split("(?<!Dr|Prof|geb|(\\d*))[;\\.]\\s(?=.*?\\d+\\.)")) // semicolon or dot followed by a birthdate somewhere later
    .flatMap(s => s.split("(?<=\\))\\.")) // split . preceeded by ) == bla gmbh (Amtsgericht Frankfurt am Main HRB 54743). 
    .flatMap(s => s.split("(\\.|,)\\s?(?=bestellt als |Neu bestellt als |Bestellt|Einzelprokura|Gesamtprokura|Erteilt|Nach |Personenbezogene |Personendaten |Wohnort|Geändert |Vertretung geändert|Vertretungsbefugnis berichtigt:|Die Eintragung|Änderung zu)"))
    .flatMap(s => s.split("(?<=\\))\\s(?=Neu bestellt als|Bestellt|Einzelprokura|Gesamtprokura|Erteilt)")) // split space preceeded by ) and followed by prokura ( Amtsgericht) Einzeprokura.. )
    .flatMap(s => s.split("(?=Prokura: Nicht mehr|Prokura: War Prokurist :|Nicht mehr|Prokura erloschen:|Bestellt als)"))
    .flatMap(s => s.split("\\. ?(Geändert, nun:|Geändert:|Geändert, nunmehr:|Nach Änderung der Vertretungsregelung weiterhin:|Nach Änderung der Vertretungsbefugnis:|Nach Änderung der Vertretungsregelung, jetzt:|Jeweils nach Änderung der besonderen Vertretungsbefugnis, weiterhin:|Nach Änderung der Vertretungsbefugnis nunmehr:|Nach Änderung der Vertretungsbefugnis weiterhin:)"))
    .flatMap(s => s.split("(Geändert, nunmehr bestellt als|Nunmehr bestellt als|Geändert, nunmehr|\\. geändert nun |Geändert nun |Geändert, nun |Vertretungsbefugnis geändert, nun |Nunmehr bestellt als|Vertretungsbefugnis geändert, nunmehr):?"))
    .flatMap(s => {
      if(s.count(_ == STAR) == 2 && s.contains(" und ")){
        s.split(" und ")
      }else{
        Array(s)
      }
    })
    .map(str => str.takeRight(1) match {
      case "." => str.dropRight(1)
      case _ => str
    })
  .filter(str => str.trim.length > 1) // drop empty strings and commas, dots etc
  .map(str => str.trim)
  // drop these type of listings:
  //Vorstand: Gehrt, Markus, Tübingen, *23.05.1967, mit der Befugnis, im Namen der Gesellschaft mit sich als Vertreter folgender Gesellschaften Rechtsgeschäfte abzuschließen: - Metabowerke GmbH, Nürtingen; - Metabo Grundstücksverwaltung Beteiligungs-GmbH. Nürtingen; - Metabo Grundstücksverwaltung GmbH & Co. KG, Nürtingen; - Elektra Beckum GmbH, Nürtingen; - Metabo S.A.S., Montigny-Ie-Bretonneux, Frankreich; - Metabo Nederland B.V., Breukelen, Niederlande; - Herramientas Metabo S.A., Madrid, Spanien...
 .filter(str => !str.startsWith("-"))
  
  
  // post processing
  // one issue: sometimes (because of the mit der befugnis regex), we split off the prokura sting (eg Gesamtprokura gemeinsam mit einem Geschäftsführer oder einem anderen Prokuristen mit der Befugnis, im Namen der Gesellschaft mit sich im eigenen Namen oder als Vertreter eines Dritten Rechtsgeschäfte abzuschließen: Hantke, Sigrid, Hamburg, *15.02.1951.)
  // as a hacky solution we join back these strings to the beginning if the string starts with a :
  val officerListBuffer = ArrayBuffer[String]()
  val officerStringsReverse = officerStrings.reverse
  var skipNext = false
  
  for(i <- 0 until officerStringsReverse.length) {
    if (!skipNext) {
      if(officerStringsReverse(i).startsWith(":") && ((officerStringsReverse(i + 1).contains("prokura") || officerStringsReverse(i + 1).contains("Prokura")) || (officerStringsReverse(i + 1).contains("Bestellt") && officerStringsReverse(i + 1).contains("Geschäftsführer")))) {
        val newStr = officerStringsReverse(i + 1) ++ officerStringsReverse(i)
        officerListBuffer += newStr
        skipNext = true
      } else {
        officerListBuffer += officerStringsReverse(i)
      }
    } else {
      skipNext = false
    }
  }
  
  officerListBuffer.reverse
  //officerStrings
} //: ArrayBuffer[String]



val splitInlineProkura = (os: String) => {
  val colon: Char = ":".charAt(0)
  val isInlineProkuraString = (s: String) => os.contains(" Prokura: ") && (os.count(_ == colon) == 2)
  val r = isInlineProkuraString(os) match {
    case true => os.split("(?= Prokura: )")
    case _ => Array(os)
  }
  
  r
}: Array[String]

val cleanDoubleColons = (officerStr: String) => {
  officerStr
  .replace("Prokura: Nicht mehr Prokurist :", "Nicht mehr Prokurist:")
  .replace("Prokura: Nicht mehr Prokurist:", "Nicht mehr Prokurist:")
  .replace("Vorstand: Nicht mehr Geschäftsführer:", "Nicht mehr Geschäftsführer:")
  .replace("Vorstand: Geschäftsführer:", "Geschäftsführer:")
  .replace("Prokura: Nicht mehr Prokurist:", "Nicht mehr Prokura:")
  .replace("Prokura: War Prokurist:", "Nicht mehr Prokurist:")
}

val postprocessDoubleColonStrings = udf((officerStrings: Array[String]) => {
  officerStrings.map(s => cleanDoubleColons(s)).flatMap(s => splitInlineProkura(s))
})


// TODO Gesamtprokura case (is not split properly, but contained in last item in array)

val getPeopleStrStartRegex = (titleTokens: String) => {
  s"(?i)(((Ausgeschieden|Bestellt|War|Nicht mehr)?\\s?($titleTokens)( erloschen)?:)|Prokura erloschen)(.*)"
}: String

spark.udf.register("getTitleTokens", udf(getTitleTokens))
val getTitleTokensUdf = udf(getTitleTokens)
val splitOfficer = udf(splitOfficerStr)
spark.udf.register("splitOfficer", splitOfficer)
spark.udf.register("getPeopleStrStartRegex", udf(getPeopleStrStartRegex))

// COMMAND ----------

case class Officer(position: String = "", positionQualifier: Option[String] = None, firstName: Option[String] = None, lastName: Option[String] = None, maidenName: Option[String] = None, birthDate: Option[String] = None, city: Option[String] = None, changeStr: Option[String] = None, court: Option[String] = None, remainder: Option[String] = None, isCompany: Boolean = false, internalNumber: Option[Int] = None, inputString: String = "")

//case class Officer(role:String, firstName: String, lastName: String, city: String, birthDate: String, pattern: String = "XXX", court: String = "XXX", flags: Flags = null, internalNumber: String = "", country : String = "Deutschland")

// todo:
// write first function to split: officer / company
// then depending on result run appropirate func and return either officer or company


// Eingetreten als Persönlich haftender Gesellschafter: GBV Verwaltungs UG (haftungsbeschränkt), Papenburg (Amtsgericht Osnabrück HRB 210017)

val standardizePosition = (position: String) => {
  position 
}

//Nicht mehr Persönlich haftender Gesellschafter : 1. Horst Lehmann GmbH
val extractOneCompany = (officer: String) => {
  var position = ""
  var court: Option[String] = None
  var city: Option[String] = None
  
  var companyRest3 = officer
  var positionQualifier: Option[String] = None
  val companySplit2 = officer.split(":").map(e => e.trim())
  if(companySplit2.length == 2) {
    position = companySplit2(0)
    companyRest3 = companySplit2(1)
  } else if (companySplit2.length == 3) {
    positionQualifier = Some(companySplit2(0))
    position = companySplit2(1)
    companyRest3 = companySplit2(2)    
  }
  
  val companySplit3 = companyRest3.split("(,(?!\\s?HRB))|mit Sitz in").map(_.trim) // split token for city, comma not followed by hrb 
  val companyName = Some(companySplit3(0))


  if(companySplit3.length > 1) {
    var companySplit1 = companySplit3(1).split("\\(").map(e => e.trim())
    var companyRest1 = companySplit3(1)


    if(companySplit1.length == 2) {
      city = Some(companySplit1(0))
      val courtString = companySplit1(1).replace(")", "").trim()
      court = Some(courtString)
    } else {
      city = Some(companySplit1(0))
    }
  }
  
  Officer(firstName=companyName, court=court, position=position, city=city, isCompany=true, inputString=officer)
}: Officer

val extractOneOfficer = (officer: String) => {
  // cut off position if available (before :)
  // cut off birthdate if available (*)
  // last name, first name 
  // remainder is city / country

  // how to seperate people from companies?
  var officerSplit1 = officer.split(":").map(e => e.trim())
  var position = ""
  var positionQualifier: Option[String] = None
  var city: Option[String] = None
  var birthDate: Option[String] = None
  var changeStr: Option[String] = None
  var maidenName: Option[String] = None
  var officerRest1 = officer

  val changeStrArr = officerSplit1.filter(e => e.contains("Änderung") || e.contains("berichtigt"))
  if(changeStrArr.length == 1) {
    changeStr = Some(changeStrArr(0))
  }

  officerSplit1 = officerSplit1.filter(e => !e.contains("Änderung"))

  if (officerSplit1.length == 2) {
    position = officerSplit1(0)
    officerRest1 = officerSplit1(1)
  } else if (officerSplit1.length == 3) {

    positionQualifier = Some(officerSplit1(0))
    position = officerSplit1(1)
    officerRest1 = officerSplit1(2)
  }

  val officerSplit2 = officerRest1.split(",").map(e => e.trim())
  var lastName = officerSplit2(0)
  
  val internalNumberPatternFind = s".*([0-9]+)\\.?.*".r
  val internalNumberPatternReplace = s"([0-9]+\\.?)".r
  val internalNumber = lastName match { case internalNumberPatternFind(number) => Some(number.toInt) case _ => None }
  
  if(internalNumber.isDefined) {
    lastName = internalNumberPatternReplace.replaceFirstIn(lastName, "").trim()
  }
  
  val firstName = Some(officerSplit2(1))

  val officerRemainder = officerSplit2.drop(2)

  val maidenArr = officerRemainder.filter(e => e.contains("geb"))
  val cityArr = officerRemainder.filter(e => !e.contains("*") && !e.contains("geb"))
  val bdayArr = officerRemainder.filter(e => e.contains("*"))

  if(cityArr.length > 0) {
    city = Some(cityArr(0))
  }


  if(bdayArr.length > 0) {
    birthDate = Some(bdayArr(0).replace("*", "").trim())
  }

  if(maidenArr.length > 0) {
    maidenName = Some(maidenArr(0))
  }

  val finalRemainder = officerRemainder.filter(e => !e.contains(birthDate.getOrElse("_____")) && !e.contains(city.getOrElse("_____")) && !e.contains("geb"))

  Officer(position=position, positionQualifier=positionQualifier, firstName=firstName, lastName=Some(lastName), birthDate=birthDate, city=city, changeStr=changeStr, maidenName=maidenName, remainder=Some(finalRemainder.mkString(",")), internalNumber=internalNumber, inputString=officer)
}: Officer


val extractOfficerOrCompany = udf((officer: String) => {
  try {
    val officerLower = officer.toLowerCase
    
    val isCompany = officer.contains("HRB") || officerLower.contains(" mbh") || officer.contains(" AG") || officerLower.contains("gmbh") || officerLower.contains("e.k.") || (officerLower.contains("persönlich haftender gesellschafter") && !officerLower.contains("*"))
    if (isCompany) {
      // company logic
      extractOneCompany(officer)
    } else if (officerLower.contains("prokura erloschen")) {
      val position = "Prokura erloschen"
      val officerObj = extractOneOfficer(officer.replace("Prokura erloschen", "").trim())
      officerObj.copy(position=position)
    } else {
      extractOneOfficer(officer)
    }
  } catch {
    case _: Throwable => Officer("PERSON_EXTRACTION_ERROR", inputString=officer)
  }
}: Officer)



spark.udf.register("extractOfficerOrCompany", extractOfficerOrCompany)
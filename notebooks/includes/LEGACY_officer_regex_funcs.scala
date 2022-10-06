// Databricks notebook source


val accented = "[a-zA-Z\\u00C0-\\u024F\\u1E00-\\u1EFF]"


case class Flags(prokura: Boolean = false, owner: Boolean = false, board: Boolean = false, previousRole: String = "", previousRoleIsProkura: Boolean = false, inputString: String = "")

case class Officer(role:String, firstName: String, lastName: String, city: String, birthDate: String, pattern: String = "XXX", court: String = "XXX", flags: Flags = null, internalNumber: String = "", country : String = "Deutschland")


val berlinPersonPattern =  "(.*?):;?\\s?\\d?\\.?\\s?(.*?)\\*(\\d+\\.\\d+\\.\\d+)\\,\\s?(.*)".r
val defaultPattern = "(.*?):;? (.*?), (.*?), \\*(\\d+\\.\\d+\\.\\d+), (.*)".r // simplified berlin, prob redundant
val secondPersonPattern = "(.*?),(.*?),(.*?),\\s?\\*(\\d+\\.\\d+\\.\\d+).*".r
val alternativePersonPattern = "(.*):(.*?),(.*?),(.*?),\\s?\\*(\\d+\\.\\d+\\.\\d+).*".r
val noCityPattern = "(.*?):(.*?),(.*?), \\*(\\d+\\.\\d+\\.\\d+)".r
val noCitySecondPersonPattern = "(?<!: )(.*?),(.*?), \\*(\\d+\\.\\d+\\.\\d+)".r
val companyPattern = "(.*):(.*?),(.*?)\\((.*?)\\).*".r
val secondCompanyPattern = "(.*?),(.*?)\\((.*?)\\).*".r

val foreignCompanyPattern = "(Persönlich haftender Gesellschafter|Persönlich haftende Gesellschafterin)\\:\\s?(.*?)\\,(?! INC)(.*?)\\((.*?)\\).*".r
val prokuraAltPattern = s"(Prokuristen.*?|Gesamtprokura.*?|Prokura.*?|Einzelprokura.*?)[\\.:,].*?($accented+),.*?($accented+),.*?(.*),.\\*(\\d+\\.\\d+\\.\\d+)".r
val prokuraPatternWithCity = s"(Prokuristen.*?|Gesamtprokura.*?|Prokura.*?|Einzelprokura.*?)[\\.:,].*?($accented+),.*?($accented+),.*?\\*(\\d+\\.\\d+\\.\\d+), (.*).".r
val prokuraPatternWithBirthdateAndWithoutCity = s"(Prokuristen.*?|Gesamtprokura.*?|Prokura.*?|Einzelprokura.*?)[\\.:,].*?($accented+),.*?($accented+),.*?\\*(\\d+\\.\\d+\\.\\d+).".r
val prokuraPatternWithCityAndWithoutBirthdate = s"(Prokuristen.*?|Gesamtprokura.*?|Prokura.*?|Einzelprokura.*?)[\\.:,].*?($accented+),.*?($accented+),.*?\\*(\\d+\\.\\d+\\.\\d+).".r
val prokuraNameCity = s"(.*?),.*?(.*?),.*?(.*).".r


val prokuraExpired = "Prokura erloschen (.*?), (.*?),.*".r
val foreignDirectorPattern = "(.*?): (.*?), (.*?), \\*(\\d+\\.\\d+\\.\\d+), (.*?), (.*)".r
val foreignDirectorPattern2 = "(.*?): (.*?), (.*?), \\*(\\d+\\.\\d+\\.\\d+), (.*?)\\ ?/ ?(.*)".r
val secondForeignDirectorPattern2 = "(.*?), (.*?), \\*(\\d+\\.\\d+\\.\\d+), (.*?)\\ ?/ ?(.*)".r

val formerPattern = s"(.*?:;? )?(Nicht .*?|War .*?):;?(.*?), (.*)".r
val formerNameOnly = "(.*?), (.*)(?![,])".r
val formerNameCity = "(.*?), (.*?), (.*)(?![,])".r
val formerCompanyNameOnly = "(Nicht mehr [Pp]ersönlich haftender Gesellschafter):(.*)(?![,\\(\\)])".r

val nameOnlyPattern = s"(.*?): (.*?), (.*)".r

val extractOfficers = udf((officers: Array[String]) => {
  val ret = ArrayBuffer[Officer]()
  // todo replace with .*?
  try {
    for(i <- 0 until officers.length) {
      val officerPre = officers(i).trim()
      val board = officerPre.contains("Vorstand")
      val owner = officerPre.contains("Inhaber")
      val hasRole = officerPre.contains(":")
      var previousRole: String = ""

      if (!hasRole) {
        previousRole = ret.map(r => r.role).filter(r => r != null).last
      }

      val previousRoleIsProkura = previousRole.toLowerCase().contains("prokura")
      val prokura = officerPre.contains("Prokura") || (!hasRole && previousRoleIsProkura)
      val flags = Flags(prokura, owner, board, previousRole, previousRoleIsProkura, officerPre)

      // vorstand: geschäftsführer: bla, bla => replace "vorstand:"
      // keep: vorstand: bla, bla, *12.12.1222

      val vorstandPrefixPattern = "Vorstand: .*:.*".r
      val prokuraPrefixPattern = "Prokura:.*:.*".r
      val ownerPrefixPattern = "Inhaber:.*:.*".r
      val officer : String = officerPre match {
        case vorstandPrefixPattern() => officerPre.replace("Vorstand:", "").trim()
        case prokuraPrefixPattern() => officerPre.replace("Prokura:", "").trim()
        case ownerPrefixPattern() => officerPre.replace("Inhaber:", "").trim()
        case _ => officerPre
      }

      val numberPattern = ": ([0-9+])\\.".r
      val internalNumber = numberPattern.findFirstIn(officer).getOrElse("")

      try {
        officer match {
          case prokuraAltPattern(role, lastName, firstName, city, birthDate) if (prokura || previousRoleIsProkura) => ret += Officer(role=role, firstName=firstName, lastName=lastName, city=city, birthDate=birthDate, pattern="prokuraAltPattern", flags=flags, internalNumber=internalNumber)
          
          case prokuraPatternWithCity(role, lastName, firstName, birthDate, city) if (prokura || previousRoleIsProkura) => ret += Officer(role=role, firstName=firstName, lastName=lastName, city=city, birthDate=birthDate, pattern="prokuraPatternWithCity", flags=flags, internalNumber=internalNumber)

          case prokuraPatternWithBirthdateAndWithoutCity(role, lastName, firstName, birthDate) if (prokura || previousRoleIsProkura) => ret += Officer(role=role, firstName=firstName, lastName=lastName, city="XXX", birthDate=birthDate, pattern="prokuraPatternWithBirthdateAndWithoutCity", flags=flags, internalNumber=internalNumber)
          
          case prokuraPatternWithCityAndWithoutBirthdate(role, lastName, firstName, city) if (prokura || previousRoleIsProkura) => ret += Officer(role=role, firstName=firstName, lastName=lastName, city=city, birthDate="XXX", pattern="prokuraPatternWithBirthdateAndWithoutCity", flags=flags, internalNumber=internalNumber)
          
          case prokuraNameCity(lastName, firstName, city) if previousRoleIsProkura => ret += Officer(role="XXX", firstName=firstName, lastName=lastName, city=city, birthDate="XXX", pattern="prokuraNameCity", flags=flags, internalNumber=internalNumber)

          case foreignDirectorPattern(role, lastName, firstName, birthDate, city, country) =>  ret += Officer(role=role, firstName=firstName, lastName=lastName, city=city, birthDate=birthDate, pattern="foreignDirectorPattern", flags=flags, internalNumber=internalNumber, country=country)

          case foreignDirectorPattern2(role, lastName, firstName, birthDate, city, country) =>  ret += Officer(role=role, firstName=firstName, lastName=lastName, city=city, birthDate=birthDate, pattern="foreignDirectorPattern2", flags=flags, internalNumber=internalNumber, country=country)

          case secondForeignDirectorPattern2(lastName, firstName, birthDate, city, country) if i > 0 =>  ret += Officer(role="XXX", firstName=firstName, lastName=lastName, city=city, birthDate=birthDate, pattern="secondForeignDirectorPattern2", flags=flags, internalNumber=internalNumber, country=country)

          //case defaultPattern(role, lastName, firstName, birthDate, city) =>  ret += Officer(role=role, firstName=firstName, lastName=lastName, city=city, birthDate=birthDate, pattern="defaultPattern", board=board, internalNumber=internalNumber)
          case berlinPersonPattern(role, name, birthDate, city) =>  ret += Officer(role=role, firstName=name.split(",")(1), lastName=name.split(",")(0), city=city, birthDate=birthDate, pattern="berlinPersonPattern", flags=flags, internalNumber=internalNumber)

          case alternativePersonPattern(role, lastName, firstName, city, birthDate) => ret += Officer(role=role, firstName=firstName, lastName=lastName, city=city, birthDate=birthDate, pattern="alternativePersonPattern", flags=flags, internalNumber=internalNumber)

          case companyPattern(role, companyName, city, court) => ret += Officer(role=role, firstName=companyName, lastName="XXX", city=city, birthDate="XXX", court=court, pattern="companyPattern", internalNumber=internalNumber, flags=flags)

          case secondCompanyPattern(companyName, city, court) if i > 0 => ret += Officer(role="XXX", firstName=companyName, lastName="XXX", city=city, birthDate="XXX", court=court, pattern="secondCompanyPattern", internalNumber=internalNumber, flags=flags)

          case foreignCompanyPattern(role, companyName, city, court) => ret += Officer(role=role, firstName=companyName, lastName="XXX", city=city, birthDate="XXX", court=court, pattern="foreignCompanyPattern", flags=flags, internalNumber=internalNumber)

          case secondPersonPattern(lastName, firstName, city, birthDate) if i > 0 =>  ret += Officer(role="XXX", firstName=firstName, lastName=lastName, city=city, birthDate=birthDate, pattern="secondPersonPattern", flags=flags, internalNumber=internalNumber)

          case noCityPattern(role, lastName, firstName, birthDate) =>  ret += Officer(role=role, firstName=firstName, lastName=lastName, city="XXX", birthDate=birthDate, pattern="noCityPattern", flags=flags)

          case noCitySecondPersonPattern(lastName, firstName, birthDate) if i > 0 =>  ret += Officer(role="XXX", firstName=firstName, lastName=lastName, city="XXX", birthDate=birthDate, pattern="noCitySecondPersonPattern", flags=flags, internalNumber=internalNumber)

          case formerPattern(section, role, lastName, firstName) => ret += Officer(role=role, firstName=firstName, lastName=lastName, city="XXX", birthDate="XXX", pattern="formerPattern", flags=flags, internalNumber=internalNumber)

          case nameOnlyPattern(role, lastName, firstName) => ret += Officer(role=role, firstName=firstName, lastName=lastName, city="XXX", birthDate="XXX", pattern="nameOnlyPattern", flags=flags, internalNumber=internalNumber)

          case formerNameOnly(lastName, firstName) if i > 0 => ret += Officer(role="XXX", firstName=firstName, lastName=lastName, city="XXX", birthDate="XXX", pattern="formerNameOnly", flags=flags, internalNumber=internalNumber)

          case formerNameCity(lastName, firstName, city) if i > 0 => ret += Officer(role="XXX", firstName=firstName, lastName=lastName, city=city, birthDate="XXX", pattern="formerNameCity", flags=flags, internalNumber=internalNumber)

          case formerCompanyNameOnly(role, companyName) => ret += Officer(role="XXX", firstName=companyName, lastName="XXX", city="XXX", birthDate="XXX", pattern="formerCompanyNameOnly", flags=flags, internalNumber=internalNumber)

          case _ => Officer("MATCH FAIL", officer, "X", "X", "X", "X") // ret +=
        }
      } catch {
        case e: Exception => ret += Officer("X", "X", "X", "X", "X", "X")
      }
    }
  } catch {
    case e: Exception => null
  }
  ret
}: ArrayBuffer[Officer])
spark.udf.register("extractOfficers", extractOfficers)

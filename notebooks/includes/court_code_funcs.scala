// Databricks notebook source
// CODELISTE HIER: https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:gds.gerichte_3.5/download/GDS.Gerichte_3.5.json
// in js: j.daten.filter(([code, court]) => court.includes("Amtsgericht")).map(([code, court]) => ({code, court}))


// COMMAND ----------

// MAGIC %md
// MAGIC grab all court codes from the court select on https://www.handelsregisterbekanntmachungen.de/index.php?aktion=suche#Ergebnis

// COMMAND ----------

case class Court(code:String, name: String, aliases: List[String], legacy: Boolean = false)

/* js code to generate:
s = Array.from(document.querySelectorAll("select[name='registergericht'] option")).map(elem => `Court(code="${elem.value}", name="${elem.innerText}", aliases=List("${elem.innerText.toLowerCase()}"), legacy=true)`).slice(1)
run on: https://www.handelsregister.de/rp_web/mask.do?Typ=n */

val courtList = List(
Court(code="R3101", name="Aachen", aliases=List("aachen"), legacy=true),
Court(code="Y1201", name="Altenburg", aliases=List("altenburg"), legacy=true),
Court(code="D3101", name="Amberg", aliases=List("amberg"), legacy=true),
Court(code="D3201", name="Ansbach", aliases=List("ansbach"), legacy=true),
Court(code="Y1101", name="Apolda", aliases=List("apolda"), legacy=true),
Court(code="R1901", name="Arnsberg", aliases=List("arnsberg"), legacy=true),
Court(code="Y1102", name="Arnstadt", aliases=List("arnstadt"), legacy=true),
Court(code="Y1303", name="Arnstadt Zweigstelle Ilmenau", aliases=List("arnstadt zweigstelle ilmenau"), legacy=true),
Court(code="D4102", name="Aschaffenburg", aliases=List("aschaffenburg"), legacy=true),
Court(code="D2102", name="Augsburg", aliases=List("augsburg"), legacy=true),
Court(code="P3101", name="Aurich", aliases=List("aurich"), legacy=true),
Court(code="M1305", name="Bad Hersfeld", aliases=List("bad hersfeld"), legacy=true),
Court(code="M1202", name="Bad Homburg v.d.H.", aliases=List("bad homburg v.d.h.", "bad homburg", "bad homburg v. d. höhe", "bad homburg von der höhe", "bad homburg v.d. höhe"), legacy=true),
Court(code="T2101", name="Bad Kreuznach", aliases=List("bad kreuznach"), legacy=true),
Court(code="R2108", name="Bad Oeynhausen", aliases=List("bad oeynhausen"), legacy=true),
Court(code="Y1301", name="Bad Salzungen", aliases=List("bad salzungen"), legacy=true),
Court(code="D4201", name="Bamberg", aliases=List("bamberg"), legacy=true),
Court(code="D4301", name="Bayreuth", aliases=List("bayreuth"), legacy=true),
Court(code="F1103", name="Berlin (Charlottenburg)", aliases=List("charlottenburg", "berlin", "charlottenburg (berlin)", "berlin charlottenburg", "berlin-charlottenburg", "charlottenburg"), legacy=true),
Court(code="R2101", name="Bielefeld", aliases=List("bielefeld"), legacy=true),
Court(code="R2201", name="Bochum", aliases=List("bochum"), legacy=true),
Court(code="R3201", name="Bonn", aliases=List("bonn"), legacy=true),
Court(code="P1103", name="Braunschweig", aliases=List("braunschweig"), legacy=true),
Court(code="H1101", name="Bremen", aliases=List("bremen", "bremerhaven"), legacy=true),
Court(code="U1206", name="Chemnitz", aliases=List("chemnitz"), legacy=true),
Court(code="D4401", name="Coburg", aliases=List("coburg"), legacy=true),
Court(code="R2707", name="Coesfeld", aliases=List("coesfeld"), legacy=true),
Court(code="G1103", name="Cottbus", aliases=List("cottbus"), legacy=true),
Court(code="M1103", name="Darmstadt", aliases=List("darmstadt"), legacy=true),
Court(code="D2201", name="Deggendorf", aliases=List("deggendorf"), legacy=true),
Court(code="R2402", name="Dortmund", aliases=List("dortmund"), legacy=true),
Court(code="U1104", name="Dresden", aliases=List("dresden"), legacy=true),
Court(code="R1202", name="Duisburg", aliases=List("duisburg"), legacy=true),
Court(code="R3103", name="Düren", aliases=List("düren"), legacy=true),
Court(code="R1101", name="Düsseldorf", aliases=List("düsseldorf"), legacy=true),
Court(code="Y1105", name="Eisenach", aliases=List("eisenach"), legacy=true),
Court(code="Y1106", name="Erfurt", aliases=List("erfurt"), legacy=true),
Court(code="M1602", name="Eschwege", aliases=List("eschwege"), legacy=true),
Court(code="R2503", name="Essen", aliases=List("essen"), legacy=true),
Court(code="X1112", name="Flensburg", aliases=List("flensburg"), legacy=true),
Court(code="M1201", name="Frankfurt am Main", aliases=List("frankfurt am main", "frankfurt"), legacy=true),
Court(code="G1207", name="Frankfurt/Oder", aliases=List("frankfurt/oder", "frankfurt (oder)", "frankfurt(oder)"), legacy=true),
Court(code="B1204", name="Freiburg", aliases=List("freiburg"), legacy=true),
Court(code="M1405", name="Friedberg", aliases=List("friedberg", "friedberg (hessen)"), legacy=true),
Court(code="M1603", name="Fritzlar", aliases=List("fritzlar"), legacy=true),
Court(code="M1301", name="Fulda", aliases=List("fulda"), legacy=true),
Court(code="D3304", name="Fürth", aliases=List("fürth"), legacy=true),
Court(code="R2507", name="Gelsenkirchen", aliases=List("gelsenkirchen"), legacy=true),
Court(code="Y1203", name="Gera", aliases=List("gera"), legacy=true),
Court(code="M1406", name="Gießen", aliases=List("gießen"), legacy=true),
Court(code="Y1108", name="Gotha", aliases=List("gotha"), legacy=true),
Court(code="P2204", name="Göttingen", aliases=List("göttingen"), legacy=true),
Court(code="Y1205", name="Greiz", aliases=List("greiz"), legacy=true),
Court(code="R2103", name="Gütersloh", aliases=List("gütersloh"), legacy=true),
Court(code="R2602", name="Hagen", aliases=List("hagen"), legacy=true),
Court(code="K1101", name="Hamburg", aliases=List("hamburg"), legacy=true),
Court(code="R2404", name="Hamm", aliases=List("hamm"), legacy=true),
Court(code="M1502", name="Hanau", aliases=List("hanau"), legacy=true),
Court(code="P2305", name="Hannover", aliases=List("hannover"), legacy=true),
Court(code="Y1109", name="Heilbad Heiligenstadt", aliases=List("heilbad heiligenstadt", "heiligenstadt"), legacy=true),
Court(code="Y1302", name="Hildburghausen", aliases=List("hildburghausen"), legacy=true),
Court(code="P2408", name="Hildesheim", aliases=List("hildesheim"), legacy=true),
Court(code="D4501", name="Hof", aliases=List("hof"), legacy=true),
Court(code="V1102", name="Homburg", aliases=List("homburg"), legacy=true),
Court(code="D5701", name="Ingolstadt", aliases=List("ingolstadt"), legacy=true),
Court(code="R2604", name="Iserlohn", aliases=List("iserlohn"), legacy=true),
Court(code="Y1206", name="Jena", aliases=List("jena"), legacy=true),
Court(code="T3201", name="Kaiserslautern", aliases=List("kaiserslautern"), legacy=true),
Court(code="M1607", name="Kassel", aliases=List("kassel"), legacy=true),
Court(code="D2304", name="Kempten (Allgäu)", aliases=List("kempten (allgäu)", "kempten"), legacy=true),
Court(code="X1517", name="Kiel", aliases=List("kiel"), legacy=true),
Court(code="R1304", name="Kleve", aliases=List("kleve"), legacy=true),
Court(code="T2210", name="Koblenz", aliases=List("koblenz"), legacy=true),
Court(code="R3306", name="Köln", aliases=List("köln"), legacy=true),
Court(code="M1203", name="Königstein", aliases=List("königstein", "königstein im taunus"), legacy=true),
Court(code="M1608", name="Korbach", aliases=List("korbach"), legacy=true),
Court(code="R1402", name="Krefeld", aliases=List("krefeld"), legacy=true),
Court(code="T3304", name="Landau", aliases=List("landau", "landau in der pfalz"), legacy=true),
Court(code="D2404", name="Landshut", aliases=List("landshut"), legacy=true),
Court(code="R1105", name="Langenfeld", aliases=List("langenfeld"), legacy=true),
Court(code="V1103", name="Lebach", aliases=List("lebach"), legacy=true),
Court(code="U1308", name="Leipzig", aliases=List("leipzig"), legacy=true),
Court(code="R2307", name="Lemgo", aliases=List("lemgo"), legacy=true),
Court(code="M1706", name="Limburg", aliases=List("limburg"), legacy=true),
Court(code="X1721", name="Lübeck", aliases=List("lübeck"), legacy=true),
Court(code="T3104", name="Ludwigshafen a.Rhein (Ludwigshafen)", aliases=List("ludwigshafen am rhein", "ludwigshafen a. rhein", "ludwigshafen a r", "ludwigshafen", "ludwigshafen a.rhein", "ludwigshafen a.rhein (ludwigshafen)"), legacy=true),
Court(code="P2507", name="Lüneburg", aliases=List("lüneburg"), legacy=true),
Court(code="T2304", name="Mainz", aliases=List("mainz"), legacy=true),
Court(code="B1601", name="Mannheim", aliases=List("mannheim"), legacy=true),
Court(code="M1809", name="Marburg", aliases=List("marburg"), legacy=true),
Court(code="Y1304", name="Meiningen", aliases=List("meiningen"), legacy=true),
Court(code="D2505", name="Memmingen", aliases=List("memmingen"), legacy=true),
Court(code="V1104", name="Merzig", aliases=List("merzig"), legacy=true),
Court(code="R1504", name="Mönchengladbach", aliases=List("mönchengladbach", "moenchengladbach"), legacy=true),
Court(code="T2214", name="Montabaur", aliases=List("montabaur"), legacy=true),
Court(code="Y1110", name="Mühlhausen", aliases=List("mühlhausen"), legacy=true),
Court(code="D2601", name="München", aliases=List("münchen"), legacy=true),
Court(code="R2713", name="Münster", aliases=List("münster"), legacy=true),
Court(code="N1105", name="Neubrandenburg", aliases=List("neubrandenburg"), legacy=true),
Court(code="V1105", name="Neunkirchen", aliases=List("neunkirchen"), legacy=true),
Court(code="G1309", name="Neuruppin", aliases=List("neuruppin"), legacy=true),
Court(code="R1102", name="Neuss", aliases=List("neuss"), legacy=true),
Court(code="Y1111", name="Nordhausen", aliases=List("nordhausen"), legacy=true),
Court(code="D3310", name="Nürnberg", aliases=List("nürnberg"), legacy=true),
Court(code="M1114", name="Offenbach am Main", aliases=List("offenbach am main", "offenbach"), legacy=true),
Court(code="P3210", name="Oldenburg (Oldenburg)", aliases=List("oldenburg (oldenburg)", "oldenburg"), legacy=true),
Court(code="P3313", name="Osnabrück", aliases=List("osnabrück"), legacy=true),
Court(code="V1107", name="Ottweiler", aliases=List("ottweiler"), legacy=true),
Court(code="R2809", name="Paderborn", aliases=List("paderborn"), legacy=true),
Court(code="D2803", name="Passau", aliases=List("passau"), legacy=true),
Court(code="X1321", name="Pinneberg", aliases=List("pinneberg"), legacy=true),
Court(code="Y1209", name="Pößneck", aliases=List("pößneck"), legacy=true),
Court(code="Y1208", name="Pößneck Zweigstelle Bad Lobenstein", aliases=List("pößneck zweigstelle bad lobenstein"), legacy=true),
Court(code="G1312", name="Potsdam", aliases=List("potsdam"), legacy=true),
Court(code="R2204", name="Recklinghausen", aliases=List("recklinghausen"), legacy=true),
Court(code="D3410", name="Regensburg", aliases=List("regensburg"), legacy=true),
Court(code="N1206", name="Rostock", aliases=List("rostock"), legacy=true),
Court(code="Y1210", name="Rudolstadt", aliases=List("rudolstadt"), legacy=true),
Court(code="V1109", name="Saarbrücken", aliases=List("saarbrücken"), legacy=true),
Court(code="V1110", name="Saarlouis", aliases=List("saarlouis"), legacy=true),
Court(code="D4608", name="Schweinfurt", aliases=List("schweinfurt"), legacy=true),
Court(code="N1308", name="Schwerin", aliases=List("schwerin"), legacy=true),
Court(code="R3208", name="Siegburg", aliases=List("siegburg"), legacy=true),
Court(code="R2909", name="Siegen", aliases=List("siegen"), legacy=true),
Court(code="Y1112", name="Sömmerda", aliases=List("sömmerda"), legacy=true),
Court(code="Y1113", name="Sondershausen", aliases=List("sondershausen"), legacy=true),
Court(code="Y1307", name="Sonneberg", aliases=List("sonneberg"), legacy=true),
Court(code="P2106", name="Stadthagen", aliases=List("stadthagen"), legacy=true),
Court(code="Y1214", name="Stadtroda", aliases=List("stadtroda"), legacy=true),
Court(code="R2706", name="Steinfurt", aliases=List("steinfurt"), legacy=true),
Court(code="W1215", name="Stendal", aliases=List("stendal", "magdeburg", "mdeburg", "halle-saalkreis", "halle"), legacy=true),
Court(code="V1111", name="St. Ingbert (St Ingbert)", aliases=List("st. ingbert (st ingbert)"), legacy=true),
Court(code="N1209", name="Stralsund", aliases=List("stralsund"), legacy=true),
Court(code="D3413", name="Straubing", aliases=List("straubing"), legacy=true),
Court(code="B2609", name="Stuttgart", aliases=List("stuttgart"), legacy=true),
Court(code="V1112", name="St. Wendel (St Wendel)", aliases=List("st. wendel (st wendel)"), legacy=true),
Court(code="Y1308", name="Suhl", aliases=List("suhl"), legacy=true),
Court(code="P2613", name="Tostedt", aliases=List("tostedt"), legacy=true),
Court(code="D2910", name="Traunstein", aliases=List("traunstein"), legacy=true),
Court(code="B2805", name="Ulm", aliases=List("ulm"), legacy=true),
Court(code="V1115", name="Völklingen", aliases=List("völklingen"), legacy=true),
Court(code="P2716", name="Walsrode", aliases=List("walsrode"), legacy=true),
Court(code="D3508", name="Weiden i. d. OPf.", aliases=List("weiden", "weiden i. d. opf", "weiden i. d. opf (weiden)"), legacy=true),
Court(code="Y1114", name="Weimar", aliases=List("weimar"), legacy=true),
Court(code="M1710", name="Wetzlar", aliases=List("wetzlar"), legacy=true),
Court(code="M1906", name="Wiesbaden", aliases=List("wiesbaden"), legacy=true),
Court(code="T2408", name="Wittlich", aliases=List("wittlich"), legacy=true),
Court(code="R1608", name="Wuppertal", aliases=List("wuppertal"), legacy=true),
Court(code="D4708", name="Würzburg", aliases=List("würzburg"), legacy=true),
Court(code="T3403", name="Zweibrücken", aliases=List("zweibrücken"), legacy=true),
//// NEW COURTS START HERE ////
Court(code="B1603", name="Weinheim", aliases=List("weinheim"), legacy=false),
Court(code="U1302", name="Borna", aliases=List("borna"), legacy=false),
Court(code="B2701", name="Calw", aliases=List("calw"), legacy=false),
Court(code="B1202", name="Emmendingen", aliases=List("emmendingen"), legacy=false),
Court(code="B1402", name="Bruchsal", aliases=List("bruchsal"), legacy=false),
Court(code="B2302", name="Brackenheim", aliases=List("brackenheim"), legacy=false),
Court(code="B2502", name="Horb", aliases=List("horb"), legacy=false),
Court(code="U1109", name="Hoyerswerda", aliases=List("hoyerswerda"), legacy=false),
Court(code="B1510", name="Überlingen", aliases=List("überlingen"), legacy=false),
Court(code="B2503", name="Oberndorf", aliases=List("oberndorf"), legacy=false),
Court(code="B2204", name="Hechingen", aliases=List("hechingen"), legacy=false),
Court(code="B1804", name="Oberkirch", aliases=List("oberkirch"), legacy=false),
Court(code="B1806", name="Wolfach", aliases=List("wolfach"), legacy=false),
Court(code="N1304", name="Hagenow", aliases=List("hagenow"), legacy=false),
Court(code="B2101", name="Aalen", aliases=List("aalen"), legacy=false),
Court(code="U1112", name="Meißen", aliases=List("meißen"), legacy=false),
Court(code="R2511", name="Marl", aliases=List("marl"), legacy=false),
Court(code="R1404", name="Nettetal", aliases=List("nettetal"), legacy=false),
Court(code="B1503", name="Konstanz", aliases=List("konstanz"), legacy=false),
Court(code="B1102", name="Baden-Baden", aliases=List("baden-baden"), legacy=false),
Court(code="B2402", name="Biberach", aliases=List("biberach"), legacy=false),
Court(code="B1905", name="Schopfheim", aliases=List("schopfheim"), legacy=false),
Court(code="B2401", name="Bad Waldsee", aliases=List("bad waldsee"), legacy=false),
Court(code="B1302", name="Heidelberg", aliases=List("heidelberg"), legacy=false),
Court(code="B2404", name="Ravensburg", aliases=List("ravensburg"), legacy=false),
Court(code="N1202", name="Greifswald", aliases=List("greifswald"), legacy=false),
Court(code="B2108", name="Schwäbisch Gmünd", aliases=List("schwäbisch gmünd"), legacy=false),
Court(code="B2608", name="Schorndorf", aliases=List("schorndorf"), legacy=false),
Court(code="B1701", name="Adelsheim", aliases=List("adelsheim"), legacy=false),
Court(code="B1105", name="Rastatt", aliases=List("rastatt"), legacy=false),
Court(code="B2505", name="Spaichingen", aliases=List("spaichingen"), legacy=false),
Court(code="B2506", name="Tuttlingen", aliases=List("tuttlingen"), legacy=false),
Court(code="B2103", name="Crailsheim", aliases=List("crailsheim"), legacy=false),
Court(code="B1206", name="Lörrach", aliases=List("lörrach"), legacy=false),
Court(code="B1802", name="Kehl", aliases=List("kehl"), legacy=false),
Court(code="B2708", name="Bad Urach", aliases=List("bad urach"), legacy=false),
Court(code="B1205", name="Kenzingen", aliases=List("kenzingen"), legacy=false),
Court(code="B1403", name="Ettlingen", aliases=List("ettlingen"), legacy=false),
Court(code="B2304", name="Künzelsau", aliases=List("künzelsau"), legacy=false),
Court(code="B2501", name="Freudenstadt", aliases=List("freudenstadt"), legacy=false),
Court(code="N1303", name="Güstrow", aliases=List("güstrow"), legacy=false),
Court(code="B2602", name="Böblingen", aliases=List("böblingen"), legacy=false),
Court(code="B1707", name="Wertheim", aliases=List("wertheim"), legacy=false),
Court(code="B1805", name="Offenburg", aliases=List("offenburg"), legacy=false),
Court(code="U1101", name="Bautzen", aliases=List("bautzen"), legacy=false),
Court(code="B2306", name="Maulbronn", aliases=List("maulbronn"), legacy=false),
Court(code="R2510", name="Hattingen", aliases=List("hattingen"), legacy=false),
Court(code="B2407", name="Tettnang", aliases=List("tettnang"), legacy=false),
Court(code="B1101", name="Achern", aliases=List("achern"), legacy=false),
Court(code="B2504", name="Rottweil", aliases=List("rottweil"), legacy=false),
Court(code="U1115", name="Riesa", aliases=List("riesa"), legacy=false),
Court(code="B2305", name="Marbach a. N.", aliases=List("marbach a. n."), legacy=false),
Court(code="B2703", name="Nagold", aliases=List("nagold"), legacy=false),
Court(code="B2606", name="Ludwigsburg", aliases=List("ludwigsburg"), legacy=false),
Court(code="Y1307", name="Sonneberg", aliases=List("sonneberg"), legacy=false),
Court(code="M1206", name="Frankfurt am Main Außenstelle Höchst", aliases=List("frankfurt am main außenstelle höchst"), legacy=false),
Court(code="B2104", name="Ellwangen", aliases=List("ellwangen", "ellwangen (jagst)"), legacy=false),
Court(code="B2403", name="Leutkirch", aliases=List("leutkirch"), legacy=false),
Court(code="B1203", name="Ettenheim", aliases=List("ettenheim"), legacy=false),
Court(code="B2303", name="Heilbronn", aliases=List("heilbronn"), legacy=false),
Court(code="B2202", name="Albstadt", aliases=List("albstadt"), legacy=false),
Court(code="B2107", name="Neresheim", aliases=List("neresheim"), legacy=false),
Court(code="N1211", name="Wolgast", aliases=List("wolgast"), legacy=false),
Court(code="B2307", name="Öhringen", aliases=List("öhringen"), legacy=false),
Court(code="B1906", name="Waldshut-Tiengen", aliases=List("waldshut-tiengen"), legacy=false),
Court(code="B1207", name="Müllheim", aliases=List("müllheim"), legacy=false),
Court(code="B2801", name="Ehingen", aliases=List("ehingen"), legacy=false),
Court(code="U1110", name="Kamenz", aliases=List("kamenz"), legacy=false),
Court(code="B1705", name="Mosbach", aliases=List("mosbach"), legacy=false),
Court(code="Y1115", name="Heilbad Heiligenstadt Zweigstelle Leinefelde-Worbis", aliases=List("heilbad heiligenstadt zweigstelle leinefelde-worbis"), legacy=false),
Court(code="Y1104", name="Mühlhausen Zweigstelle Bad Langensalza", aliases=List("mühlhausen zweigstelle bad langensalza"), legacy=false),
Court(code="B2705", name="Reutlingen", aliases=List("reutlingen"), legacy=false),
Court(code="R2715", name="Rheine", aliases=List("rheine"), legacy=false),
Court(code="B2405", name="Riedlingen", aliases=List("riedlingen"), legacy=false),
Court(code="B2406", name="Bad Saulgau", aliases=List("bad saulgau"), legacy=false),
Court(code="N1205", name="Ribnitz-Damgarten", aliases=List("ribnitz-damgarten"), legacy=false),
Court(code="B1401", name="Bretten", aliases=List("bretten"), legacy=false),
Court(code="B2605", name="Leonberg", aliases=List("leonberg"), legacy=false),
Court(code="B1407", name="Philippsburg", aliases=List("philippsburg"), legacy=false),
Court(code="B1703", name="Buchen", aliases=List("buchen"), legacy=false),
Court(code="B1508", name="Stockach", aliases=List("stockach"), legacy=false),
Court(code="Y1308", name="Suhl", aliases=List("suhl"), legacy=false),
Court(code="B1305", name="Wiesloch", aliases=List("wiesloch"), legacy=false),
Court(code="B1803", name="Lahr", aliases=List("lahr"), legacy=false),
Court(code="B2106", name="Langenburg", aliases=List("langenburg"), legacy=false),
Court(code="B2201", name="Balingen", aliases=List("balingen"), legacy=false),
Court(code="B1706", name="Tauberbischofsheim", aliases=List("tauberbischofsheim"), legacy=false),
Court(code="U1107", name="Görlitz", aliases=List("görlitz"), legacy=false),
Court(code="B2707", name="Tübingen", aliases=List("tübingen"), legacy=false),
Court(code="B1902", name="Bad Säckingen", aliases=List("bad säckingen"), legacy=false),
Court(code="B2308", name="Schwäbisch Hall", aliases=List("schwäbisch hall"), legacy=false),
Court(code="B1602", name="Schwetzingen", aliases=List("schwetzingen"), legacy=false),
Court(code="B2601", name="Backnang", aliases=List("backnang"), legacy=false),
Court(code="B2301", name="Besigheim", aliases=List("besigheim"), legacy=false),
Court(code="B2102", name="Bad Mergentheim", aliases=List("bad mergentheim"), legacy=false),
Court(code="B1801", name="Gengenbach", aliases=List("gengenbach"), legacy=false),
Court(code="B2105", name="Heidenheim", aliases=List("heidenheim"), legacy=false),
Court(code="B1511", name="Villingen-Schwenningen", aliases=List("villingen-schwenningen"), legacy=false),
Court(code="B1506", name="Radolfzell", aliases=List("radolfzell"), legacy=false),
Court(code="B2611", name="Waiblingen", aliases=List("waiblingen"), legacy=false),
Court(code="B1104", name="Gernsbach", aliases=List("gernsbach"), legacy=false),
Court(code="N1111", name="Ueckermünde", aliases=List("ueckermünde"), legacy=false),
Court(code="N1401", name="Stralsund Zweigstelle Bergen auf Rügen", aliases=List("stralsund zweigstelle bergen auf rügen"), legacy=false),
Court(code="B2607", name="Nürtingen", aliases=List("nürtingen"), legacy=false),
Court(code="N1102", name="Anklam", aliases=List("anklam"), legacy=false),
Court(code="U1203", name="Auerbach", aliases=List("auerbach"), legacy=false),
Court(code="U1305", name="Eilenburg", aliases=List("auerbach"), legacy=false),
Court(code="U1215", name="Plauen", aliases=List("plauen"), legacy=false),
Court(code="U1117", name="Weißwasser", aliases=List("weißwasser"), legacy=false),
Court(code="N1210", name="Wismar", aliases=List("wismar"), legacy=false),
Court(code="U1118", name="Zittau", aliases=List("zittau"), legacy=false),
Court(code="U1222", name="Zwickau", aliases=List("zwickau"), legacy=false),
Court(code="N1112", name="Waren (Müritz)", aliases=List("waren", "waren (müritz)", "müritz"), legacy=false),
Court(code="B1210", name="Waldkirch", aliases=List("waldkirch"), legacy=false),
Court(code="U1310", name="Torgau", aliases=List("torgau"), legacy=false),
Court(code="U1312", name="Torgau, Zweigstelle Oschatz", aliases=List("oschatz", "torgau zweigstelle oschatz"), legacy=false),
Court(code="N1401", name="Stralsund Zweigstelle Bergen auf Rügen", aliases=List("bergen auf rügen"), legacy=false),
Court(code="B1304", name="Sinsheim", aliases=List("sinsheim"), legacy=false),
Court(code="B1507", name="Singen", aliases=List("singen", "singen (hohentwiel)"), legacy=false),
Court(code="B2205", name="Sigmaringen", aliases=List("sigmaringen"), legacy=false),
Court(code="T3304V", name="Registergericht Landau in der Pfalz", aliases=List("landau"), legacy=false),
Court(code="B8536", name="Registergericht Freiburg i. Br.", aliases=List("freiburg", "freiburg i br", "freiburg i. br.", "freiburg im breisgau", "freiburg . br.", "freiburg i.br."), legacy=false),
Court(code="U1114", name="Pirna", aliases=List("pirna"), legacy=false),
Court(code="B1406", name="Pforzheim", aliases=List("pforzheim"), legacy=false),
Court(code="N1113", name="Pasewalk Zweigstelle Anklam", aliases=List("anklam", "paswalk-anklam", "pasewalk anklam"), legacy=false),
Court(code="N1107", name="Pasewalk", aliases=List("pasewalk"), legacy=false),
Court(code="N1307", name="Parchim", aliases=List("parchim"), legacy=false),
Court(code="B1103", name="Bühl", aliases=List("bühl"), legacy=false),
Court(code="B1501", name="Donaueschingen", aliases=List("donaueschingen"), legacy=false),
Court(code="U1304", name="Döbeln", aliases=List("döbeln"), legacy=false),
Court(code="U1305", name="Eilenburg", aliases=List("eilenburg"), legacy=false),
Court(code="U1208", name="Freiberg", aliases=List("freiberg"), legacy=false),
Court(code="N1203", name="Grevesmühlen", aliases=List("grevesmühlen"), legacy=false),
Court(code="N1203", name="Grimma", aliases=List("grimma"), legacy=false),
Court(code="B2803", name="Göppingen", aliases=List("göppingen"), legacy=false),
Court(code="U1211", name="Hohenstein-Ernstthal", aliases=List("hohenstein-ernstthal"), legacy=false),
Court(code="B1405", name="Karlsruhe-Durlach", aliases=List("karlsruhe-durlach"), legacy=false),
Court(code="B1404", name="Karlsruhe", aliases=List("karlsruhe"), legacy=false),
Court(code="U1213", name="Marienberg", aliases=List("marienberg"), legacy=false),
Court(code="B2305", name="Marbach a. N.", aliases=List("marbach"), legacy=false),
Court(code="N1305", name="Ludwigslust", aliases=List("ludwigslust"), legacy=false),
Court(code="N1103", name="Dippoldiswalde", aliases=List("dippoldiswalde"), legacy=false),
Court(code="P3210", name="Oldenburg (Oldenburg)", aliases=List("oldenburg"), legacy=false),
Court(code="N1106", name="Neustrelitz", aliases=List("neustrelitz"), legacy=false),
Court(code="N1103", name="Demmin", aliases=List("demmin"), legacy=false),
Court(code="B2702", name="Münsingen", aliases=List("münsingen"), legacy=false),
Court(code="U1111", name="Zittau Zweigstelle Löbau", aliases=List("löbau"), legacy=false),
Court(code="U1202", name="Aue-Bad Schlema", aliases=List("aue"), legacy=false),
Court(code="U1201", name="Marienberg Zweigstelle Annaberg-Buchholz", aliases=List("annaberg"), legacy=false),
Court(code="M1201", name="Frankfurt am Main", aliases=List("frankfurt am main", "frankfurt (main)", "frankfurt a main", "frankfurt a m"), legacy=false),
Court(code="U1210", name="Hainichen", aliases=List("hainichen"), legacy=false),
Court(code="B2706", name="Rottenburg am Neckar", aliases=List("rottenburg am neckar"), legacy=false),
Court(code="B1903", name="Sankt Blasien", aliases=List("sankt blasien"), legacy=false),
Court(code="U1219", name="Stollberg", aliases=List("stollberg"), legacy=false),
Court(code="B1201", name="Breisach am Rhein", aliases=List("breisach am rhein"), legacy=false),
Court(code="B2408", name="Wangen / Allgäu", aliases=List("wangen / allgäu", "wangen")),
Court(code="M1706", name="Limburg a. d. Lahn", aliases=List("limburg a. d. lahn", "limburg an der lahn", "limburg")),
Court(code="B1209", name="Staufen im Breisgau", aliases=List("staufen i. br.", "staufen", "staufen im breisgau")),
Court(code="B2309", name="Vaihingen / Enz", aliases=List("vaihingen / enz", "vaihingen")),
Court(code="B2603", name="Esslingen am Neckar", aliases=List("esslingen am neckar", "esslingen")),
Court(code="B2604", name="Kirchheim unter Teck", aliases=List("kirchheim", "kirchheim unter teck", "kircheim u. teck", "kirchheim u. t.")),
Court(code="B1904", name="Schönau im Schwarzwald", aliases=List("schönau im schwarzwald", "schönau i. s.", "schönau schwarzwald"))
)

case class CourtLookup(code: String, name:String, alias: String)


val getAliasMap = () => {
  val aliasCourtsLong = courtList.flatMap(c => c.aliases.map(alias => (alias, c)))
  val aliases: List[String] = aliasCourtsLong.map(a => a._1)
  val courts: List[Court] = aliasCourtsLong.map(a => a._2) 
  
  aliases zip courts toMap
}: Map[String, Court]
val aliasesWithCourts = getAliasMap()




import com.github.vickumar1981.stringdistance.StringDistance._


case class CourtMatch(court: String, code: String, legacy: Boolean, score: Double)

def findBestMatchingCourt(searchStr: String) = {
  // we return all strings here because otherwise there were problems with serialization in spark
  
  val lSearchStr = searchStr.toLowerCase()
  if(aliasesWithCourts.contains(searchStr)) {
    val court = aliasesWithCourts(searchStr)
    CourtMatch(court.name, court.code, court.legacy, 1.0)
    //Map("court" -> court.name, "code" -> court.code, "score" -> "1.0")
  } else {
    val aliases = aliasesWithCourts.keySet.toList
    val ngramSimilarities: List[Double] = aliases.map((alias) => NGram.score(alias, searchStr, 2))
    val maxVal = ngramSimilarities.max
    val idx = ngramSimilarities.indexOf(maxVal)
    val matchedAlias = aliases(idx)
    val court = aliasesWithCourts(matchedAlias)
    CourtMatch(court.name, court.code, court.legacy, maxVal)
    //Map("court" -> court.name, "code" -> court.code, "score" -> maxVal.toString)
  }
}: CourtMatch//Map[String, String]

val findBestMatchingCourtUdf = udf((searchStr: String) => {
  if (searchStr == null) {
    CourtMatch("ERROR", "X", false, -1.0)
  } else {
    val r: CourtMatch = findBestMatchingCourt(searchStr)
    r
  }
})

spark.udf.register("findBestMatchingCourt", findBestMatchingCourtUdf)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

val slugify = udf((input: String) => {
  import java.text.Normalizer
  Normalizer.normalize(input, Normalizer.Form.NFD)
    .replaceAll("[^\\w\\s-]", "") // Remove all non-word, non-space or non-dash characters
    .replace('-', ' ')            // Replace dashes with spaces
    .trim                         // Trim leading/trailing whitespace (including what used to be leading/trailing dashes)
    .replaceAll("\\s+", "-")      // Replace whitespace (including newlines and repetitions) with single dashes
    .toLowerCase                  // Lowercase the final results
})


def getCourtCodesDf = () => {

  import org.apache.spark.SparkFiles
  spark.sparkContext.addFile("https://www.xrepository.de/api/xrepository/urn:xoev-de:xjustiz:codeliste:gds.gerichte_3.5/download/GDS.Gerichte_3.5.json")
  val courtIdsDf = spark.read.json("file://"+SparkFiles.get("GDS.Gerichte_3.5.json"))
  .select("daten")
  .withColumn("data", explode($"daten"))
  .drop("daten")
  .withColumn("code", element_at($"data", 1))
  .withColumn("court", element_at($"data", 2))
  .drop("data")
  .filter(col("court").like("%Amtsgericht%"))
  .withColumn("court", trim(regexp_replace(regexp_replace($"court", "Amtsgericht", ""), "aufgelöst-", "")))
  .withColumn("slug", upper(slugify($"court")))
  .withColumn("mainCourtCode", regexp_extract($"code", "([A-Z]\\d+)([A-Z])?", 1))
  .withColumn("subCode", regexp_extract($"code", "[A-Z]\\d+([A-Z])", 1))
  //.filter(!col("code").isin(excludeRegisterCodes: _*))
  
  courtIdsDf
}


val courtFromReferenceNumberExpr = "TRIM(LOWER(REPLACE(element_at(split(referenceNumber, 'HR.|PR|GnR|AR|VR|Aktenzeichen:'),1), 'Amtsgericht', '')))"


def getCourtCodeReplacementsList = (aliasesWithCourts: Map[String, Court]) => { 
  val sortedAliasesWithCourts = aliasesWithCourts.toSeq.sortWith((s1, s2) => s2._1.length < s1._1.length)
  val codes = sortedAliasesWithCourts.map(cTuple => cTuple._2.code)
  val aliases = sortedAliasesWithCourts.map(cTuple => cTuple._1)
  
  val combo = aliases zip codes
  combo
}

import scala.util.matching.Regex

def massReplace = (inputCol: Column, replacements: Seq[Tuple2[String, String]]) => {
  replacements.foldLeft(inputCol)((col, strs) => regexp_replace(col, Regex.quote(strs._1), Regex.quote(strs._2)))
}

def extractHrNo(targetCol: Column) = {
  regexp_extract(targetCol, "hr[a|b] \\d+( [a-z]{1,3})?", 0)
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC // codes from the select on hrbekannt
// MAGIC 
// MAGIC val courtIdsDict = """
// MAGIC {
// MAGIC "R3101": "Aachen",
// MAGIC "Y1201": "Altenburg",
// MAGIC "D3101": "Amberg",
// MAGIC "D3201": "Ansbach",
// MAGIC "Y1101": "Apolda",
// MAGIC "R1901": "Arnsberg",
// MAGIC "Y1102": "Arnstadt",
// MAGIC "Y1303": "Arnstadt Zweigstelle Ilmenau",
// MAGIC "D4102": "Aschaffenburg",
// MAGIC "D2102": "Augsburg",
// MAGIC "P3101": "Aurich",
// MAGIC "M1305": "Bad Hersfeld",
// MAGIC "M1202": "Bad Homburg v.d.H.",
// MAGIC "T2101": "Bad Kreuznach",
// MAGIC "R2108": "Bad Oeynhausen",
// MAGIC "Y1301": "Bad Salzungen",
// MAGIC "D4201": "Bamberg",
// MAGIC "D4301": "Bayreuth",
// MAGIC "F1103": "Berlin (Charlottenburg)",
// MAGIC "R2101": "Bielefeld",
// MAGIC "R2201": "Bochum",
// MAGIC "R3201": "Bonn",
// MAGIC "P1103": "Braunschweig",
// MAGIC "H1101": "Bremen",
// MAGIC "U1206": "Chemnitz",
// MAGIC "D4401": "Coburg",
// MAGIC "R2707": "Coesfeld",
// MAGIC "G1103": "Cottbus",
// MAGIC "M1103": "Darmstadt",
// MAGIC "D2201": "Deggendorf",
// MAGIC "R2402": "Dortmund",
// MAGIC "U1104": "Dresden",
// MAGIC "R1202": "Duisburg",
// MAGIC "R3103": "Düren",
// MAGIC "R1101": "Düsseldorf",
// MAGIC "Y1105": "Eisenach",
// MAGIC "Y1106": "Erfurt",
// MAGIC "M1602": "Eschwege",
// MAGIC "R2503": "Essen",
// MAGIC "X1112": "Flensburg",
// MAGIC "M1201": "Frankfurt am Main",
// MAGIC "G1207": "Frankfurt/Oder",
// MAGIC "B1204": "Freiburg",
// MAGIC "M1405": "Friedberg",
// MAGIC "M1603": "Fritzlar",
// MAGIC "M1301": "Fulda",
// MAGIC "D3304": "Fürth",
// MAGIC "R2507": "Gelsenkirchen",
// MAGIC "Y1203": "Gera",
// MAGIC "M1406": "Gießen",
// MAGIC "Y1108": "Gotha",
// MAGIC "P2204": "Göttingen",
// MAGIC "Y1205": "Greiz",
// MAGIC "R2103": "Gütersloh",
// MAGIC "R2602": "Hagen",
// MAGIC "K1101": "Hamburg",
// MAGIC "R2404": "Hamm",
// MAGIC "M1502": "Hanau",
// MAGIC "P2305": "Hannover",
// MAGIC "Y1109": "Heilbad Heiligenstadt",
// MAGIC "Y1302": "Hildburghausen",
// MAGIC "P2408": "Hildesheim",
// MAGIC "D4501": "Hof",
// MAGIC "V1102": "Homburg",
// MAGIC "D5701": "Ingolstadt",
// MAGIC "R2604": "Iserlohn",
// MAGIC "Y1206": "Jena",
// MAGIC "T3201": "Kaiserslautern",
// MAGIC "M1607": "Kassel",
// MAGIC "D2304": "Kempten (Allgäu)",
// MAGIC "X1517": "Kiel",
// MAGIC "R1304": "Kleve",
// MAGIC "T2210": "Koblenz",
// MAGIC "R3306": "Köln",
// MAGIC "M1203": "Königstein",
// MAGIC "M1608": "Korbach",
// MAGIC "R1402": "Krefeld",
// MAGIC "T3304": "Landau",
// MAGIC "D2404": "Landshut",
// MAGIC "R1105": "Langenfeld",
// MAGIC "V1103": "Lebach",
// MAGIC "U1308": "Leipzig",
// MAGIC "R2307": "Lemgo",
// MAGIC "M1706": "Limburg",
// MAGIC "X1721": "Lübeck",
// MAGIC "T3104": "Ludwigshafen a.Rhein (Ludwigshafen)",
// MAGIC "P2507": "Lüneburg",
// MAGIC "T2304": "Mainz",
// MAGIC "B1601": "Mannheim",
// MAGIC "M1809": "Marburg",
// MAGIC "Y1304": "Meiningen",
// MAGIC "D2505": "Memmingen",
// MAGIC "V1104": "Merzig",
// MAGIC "R1504": "Mönchengladbach",
// MAGIC "T2214": "Montabaur",
// MAGIC "Y1110": "Mühlhausen",
// MAGIC "D2601": "München",
// MAGIC "R2713": "Münster",
// MAGIC "N1105": "Neubrandenburg",
// MAGIC "V1105": "Neunkirchen",
// MAGIC "G1309": "Neuruppin",
// MAGIC "R1102": "Neuss",
// MAGIC "Y1111": "Nordhausen",
// MAGIC "D3310": "Nürnberg",
// MAGIC "M1114": "Offenbach am Main",
// MAGIC "P3210": "Oldenburg (Oldenburg)",
// MAGIC "P3313": "Osnabrück",
// MAGIC "V1107": "Ottweiler",
// MAGIC "R2809": "Paderborn",
// MAGIC "D2803": "Passau",
// MAGIC "X1321": "Pinneberg",
// MAGIC "Y1209": "Pößneck",
// MAGIC "Y1208": "Pößneck Zweigstelle Bad Lobenstein",
// MAGIC "G1312": "Potsdam",
// MAGIC "R2204": "Recklinghausen",
// MAGIC "D3410": "Regensburg",
// MAGIC "N1206": "Rostock",
// MAGIC "Y1210": "Rudolstadt",
// MAGIC "V1109": "Saarbrücken",
// MAGIC "V1110": "Saarlouis",
// MAGIC "D4608": "Schweinfurt",
// MAGIC "N1308": "Schwerin",
// MAGIC "R3208": "Siegburg",
// MAGIC "R2909": "Siegen",
// MAGIC "Y1112": "Sömmerda",
// MAGIC "Y1113": "Sondershausen",
// MAGIC "Y1307": "Sonneberg",
// MAGIC "P2106": "Stadthagen",
// MAGIC "Y1214": "Stadtroda",
// MAGIC "R2706": "Steinfurt",
// MAGIC "W1215": "Stendal",
// MAGIC "V1111": "St. Ingbert (St Ingbert)",
// MAGIC "N1209": "Stralsund",
// MAGIC "D3413": "Straubing",
// MAGIC "B2609": "Stuttgart",
// MAGIC "V1112": "St. Wendel (St Wendel)",
// MAGIC "Y1308": "Suhl",
// MAGIC "P2613": "Tostedt",
// MAGIC "D2910": "Traunstein",
// MAGIC "B2805": "Ulm",
// MAGIC "V1115": "Völklingen",
// MAGIC "P2716": "Walsrode",
// MAGIC "D3508": "Weiden i. d. OPf.",
// MAGIC "Y1114": "Weimar",
// MAGIC "M1710": "Wetzlar",
// MAGIC "M1906": "Wiesbaden",
// MAGIC "T2408": "Wittlich",
// MAGIC "R1608": "Wuppertal",
// MAGIC "D4708": "Würzburg",
// MAGIC "T3403": "Zweibrücken"
// MAGIC }
// MAGIC """
// MAGIC 
// MAGIC 
// MAGIC /// Object.keys(x).map((k) => ({"code": k, "court": x[k]}))
// MAGIC val courtIdsLines = """
// MAGIC [
// MAGIC     {
// MAGIC         "code": "R3101",
// MAGIC         "court": "Aachen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1201",
// MAGIC         "court": "Altenburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D3101",
// MAGIC         "court": "Amberg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D3201",
// MAGIC         "court": "Ansbach"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1101",
// MAGIC         "court": "Apolda"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R1901",
// MAGIC         "court": "Arnsberg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1102",
// MAGIC         "court": "Arnstadt"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1303",
// MAGIC         "court": "Arnstadt Zweigstelle Ilmenau"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D4102",
// MAGIC         "court": "Aschaffenburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D2102",
// MAGIC         "court": "Augsburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "P3101",
// MAGIC         "court": "Aurich"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1305",
// MAGIC         "court": "Bad Hersfeld"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1202",
// MAGIC         "court": "Bad Homburg v.d.H."
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "T2101",
// MAGIC         "court": "Bad Kreuznach"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2108",
// MAGIC         "court": "Bad Oeynhausen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1301",
// MAGIC         "court": "Bad Salzungen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D4201",
// MAGIC         "court": "Bamberg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D4301",
// MAGIC         "court": "Bayreuth"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "F1103",
// MAGIC         "court": "Berlin (Charlottenburg)"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2101",
// MAGIC         "court": "Bielefeld"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2201",
// MAGIC         "court": "Bochum"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R3201",
// MAGIC         "court": "Bonn"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "P1103",
// MAGIC         "court": "Braunschweig"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "H1101",
// MAGIC         "court": "Bremen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "U1206",
// MAGIC         "court": "Chemnitz"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D4401",
// MAGIC         "court": "Coburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2707",
// MAGIC         "court": "Coesfeld"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "G1103",
// MAGIC         "court": "Cottbus"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1103",
// MAGIC         "court": "Darmstadt"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D2201",
// MAGIC         "court": "Deggendorf"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2402",
// MAGIC         "court": "Dortmund"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "U1104",
// MAGIC         "court": "Dresden"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R1202",
// MAGIC         "court": "Duisburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R3103",
// MAGIC         "court": "Düren"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R1101",
// MAGIC         "court": "Düsseldorf"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1105",
// MAGIC         "court": "Eisenach"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1106",
// MAGIC         "court": "Erfurt"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1602",
// MAGIC         "court": "Eschwege"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2503",
// MAGIC         "court": "Essen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "X1112",
// MAGIC         "court": "Flensburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1201",
// MAGIC         "court": "Frankfurt am Main"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "G1207",
// MAGIC         "court": "Frankfurt/Oder"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "B1204",
// MAGIC         "court": "Freiburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1405",
// MAGIC         "court": "Friedberg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1603",
// MAGIC         "court": "Fritzlar"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1301",
// MAGIC         "court": "Fulda"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D3304",
// MAGIC         "court": "Fürth"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2507",
// MAGIC         "court": "Gelsenkirchen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1203",
// MAGIC         "court": "Gera"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1406",
// MAGIC         "court": "Gießen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1108",
// MAGIC         "court": "Gotha"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "P2204",
// MAGIC         "court": "Göttingen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1205",
// MAGIC         "court": "Greiz"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2103",
// MAGIC         "court": "Gütersloh"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2602",
// MAGIC         "court": "Hagen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "K1101",
// MAGIC         "court": "Hamburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2404",
// MAGIC         "court": "Hamm"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1502",
// MAGIC         "court": "Hanau"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "P2305",
// MAGIC         "court": "Hannover"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1109",
// MAGIC         "court": "Heilbad Heiligenstadt"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1302",
// MAGIC         "court": "Hildburghausen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "P2408",
// MAGIC         "court": "Hildesheim"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D4501",
// MAGIC         "court": "Hof"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "V1102",
// MAGIC         "court": "Homburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D5701",
// MAGIC         "court": "Ingolstadt"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2604",
// MAGIC         "court": "Iserlohn"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1206",
// MAGIC         "court": "Jena"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "T3201",
// MAGIC         "court": "Kaiserslautern"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1607",
// MAGIC         "court": "Kassel"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D2304",
// MAGIC         "court": "Kempten (Allgäu)"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "X1517",
// MAGIC         "court": "Kiel"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R1304",
// MAGIC         "court": "Kleve"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "T2210",
// MAGIC         "court": "Koblenz"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R3306",
// MAGIC         "court": "Köln"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1203",
// MAGIC         "court": "Königstein"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1608",
// MAGIC         "court": "Korbach"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R1402",
// MAGIC         "court": "Krefeld"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "T3304",
// MAGIC         "court": "Landau"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D2404",
// MAGIC         "court": "Landshut"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R1105",
// MAGIC         "court": "Langenfeld"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "V1103",
// MAGIC         "court": "Lebach"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "U1308",
// MAGIC         "court": "Leipzig"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2307",
// MAGIC         "court": "Lemgo"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1706",
// MAGIC         "court": "Limburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "X1721",
// MAGIC         "court": "Lübeck"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "T3104",
// MAGIC         "court": "Ludwigshafen a.Rhein (Ludwigshafen)"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "P2507",
// MAGIC         "court": "Lüneburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "T2304",
// MAGIC         "court": "Mainz"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "B1601",
// MAGIC         "court": "Mannheim"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1809",
// MAGIC         "court": "Marburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1304",
// MAGIC         "court": "Meiningen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D2505",
// MAGIC         "court": "Memmingen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "V1104",
// MAGIC         "court": "Merzig"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R1504",
// MAGIC         "court": "Mönchengladbach"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "T2214",
// MAGIC         "court": "Montabaur"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1110",
// MAGIC         "court": "Mühlhausen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D2601",
// MAGIC         "court": "München"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2713",
// MAGIC         "court": "Münster"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "N1105",
// MAGIC         "court": "Neubrandenburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "V1105",
// MAGIC         "court": "Neunkirchen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "G1309",
// MAGIC         "court": "Neuruppin"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R1102",
// MAGIC         "court": "Neuss"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1111",
// MAGIC         "court": "Nordhausen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D3310",
// MAGIC         "court": "Nürnberg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1114",
// MAGIC         "court": "Offenbach am Main"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "P3210",
// MAGIC         "court": "Oldenburg (Oldenburg)"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "P3313",
// MAGIC         "court": "Osnabrück"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "V1107",
// MAGIC         "court": "Ottweiler"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2809",
// MAGIC         "court": "Paderborn"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D2803",
// MAGIC         "court": "Passau"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "X1321",
// MAGIC         "court": "Pinneberg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1209",
// MAGIC         "court": "Pößneck"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1208",
// MAGIC         "court": "Pößneck Zweigstelle Bad Lobenstein"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "G1312",
// MAGIC         "court": "Potsdam"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2204",
// MAGIC         "court": "Recklinghausen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D3410",
// MAGIC         "court": "Regensburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "N1206",
// MAGIC         "court": "Rostock"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1210",
// MAGIC         "court": "Rudolstadt"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "V1109",
// MAGIC         "court": "Saarbrücken"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "V1110",
// MAGIC         "court": "Saarlouis"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D4608",
// MAGIC         "court": "Schweinfurt"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "N1308",
// MAGIC         "court": "Schwerin"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R3208",
// MAGIC         "court": "Siegburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2909",
// MAGIC         "court": "Siegen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1112",
// MAGIC         "court": "Sömmerda"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1113",
// MAGIC         "court": "Sondershausen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1307",
// MAGIC         "court": "Sonneberg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "P2106",
// MAGIC         "court": "Stadthagen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1214",
// MAGIC         "court": "Stadtroda"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R2706",
// MAGIC         "court": "Steinfurt"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "W1215",
// MAGIC         "court": "Stendal"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "V1111",
// MAGIC         "court": "St. Ingbert (St Ingbert)"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "N1209",
// MAGIC         "court": "Stralsund"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D3413",
// MAGIC         "court": "Straubing"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "B2609",
// MAGIC         "court": "Stuttgart"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "V1112",
// MAGIC         "court": "St. Wendel (St Wendel)"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1308",
// MAGIC         "court": "Suhl"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "P2613",
// MAGIC         "court": "Tostedt"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D2910",
// MAGIC         "court": "Traunstein"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "B2805",
// MAGIC         "court": "Ulm"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "V1115",
// MAGIC         "court": "Völklingen"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "P2716",
// MAGIC         "court": "Walsrode"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D3508",
// MAGIC         "court": "Weiden i. d. OPf."
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "Y1114",
// MAGIC         "court": "Weimar"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1710",
// MAGIC         "court": "Wetzlar"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "M1906",
// MAGIC         "court": "Wiesbaden"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "T2408",
// MAGIC         "court": "Wittlich"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "R1608",
// MAGIC         "court": "Wuppertal"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "D4708",
// MAGIC         "court": "Würzburg"
// MAGIC     },
// MAGIC     {
// MAGIC         "code": "T3403",
// MAGIC         "court": "Zweibrücken"
// MAGIC     }
// MAGIC ]"""
# Flurstücksabschnittsverschneider (FAV)

## Installation
Der Flurstücksabschnittsverschneider ist als Skript namens `faf.pl` implementiert, der die Flächenabschnitte der Flurstücke (FAF) mit der tatsächlichen Nutzung, der öffentlich-rechtlichen und sonstigen Festlegungen, der Bewertung und der Bodenschätzung berechnet.
Das Skript erfordert perl und die zusätzlichen Pakete DBD::Pg, MIME::Lite und GNU parallel.
faf.pl kann in einem beliebigen Verzeichnis abgelegt werden. Die benötigten Module können per APT installiert werden:

`apt-get install libdbd-pg-perl libmime-lite-perl parallel`

## Ausführung
Das Skript liest seine Konfiguration von der Standardeingabe oder aus den übergebenen Dateien. Die Konfiguration kann also z.B. auf mehrere Dateien verteilt werden.

### Beispiel faf.ini:
```
db=dbname=alkis_rp port=5434
email=jef@norbit.de
#cleanup=0
#fixareas=0
#tng.p1=
#tng.p2=
#tng.p3=
#osf.p1=
#osf.p2=
#osf.p3=
#bub.p1=
#bub.p2=
#bub.p3=
#prefix=job1
```

### Beispiel faf.ctl (Modellvorgaben):
```
# Nutzungen
tng.AX_Wohnbauflaeche=Wohnbaufläche
…#
Rechtliche Festlegungen
osf.AX_KlassifizierungNachStrassenrecht.artDerFestlegung=ax_artderfestlegung_klassifizierungnac
hstrassenrecht
…#
Ausführende Stellen
osf.AX_KlassifizierungNachStrassenrecht.land|stelle=ax_dienststelle.land|stelle.bezeichnung
…#
Bodenschätzung & Bewertung
bub.AX_Bewertung.klassifizierung=ax_klassifizierung_bewertung
```

### Beispiel faf.txt (Ihre Regeldatei):
```
AX_Wohnbauflaeche|Wohnbaufläche
… AX_IndustrieUndGewerbeflaeche.funktion=2510|Förderanlage
AX_IndustrieUndGewerbeflaeche.funktion=2510&foerdergut=1000|Förderanlage - Erdöl
AX_IndustrieUndGewerbeflaeche.funktion=2510&foerdergut=2000|Förderanlage – Erdgas
… AX_Schutzzone.zone=1040.istTeilVon.AX_SchutzgebietNachWasserrecht.artDerFestlegung=1520|
Heilquellenschutzgebiet - Zone IV
…
```

### Aufruf des FAF
```
perl faf.pl faf.ini faf.ctl faf.txt
```

Beim Aufruf des Skriptes werden zu jeder zu verschneidenden Tabelle je ein SQL-Skript in einem temporären Verzeichnis erzeugt, die den angegebene Verschneidungsregeln für diese Tabellen folgen. Diese Skripte werden dann landkreisweise parallel ausgeführt. Dabei werden fünf Tabellen erzeugt und gefüllt, die mit einem konfigurierbaren Präfix versehen werden (Vorgabe `faf`).
1. `faf_fs`: Flurstücke mit `gml_id`, Kennzeichen, amtlicher und geometrischer Fläche sowie dem Schwerpunkt und der Gesamtertragszahl des Flurstücks (`gmz`)
2. `faf_tng`: Abschnitte nach „Tatsächlicher Nutzung (TNG)“ mit
   `gml_id`: des Flurstücks (Bezug zu `faf_fs`)
   `flaeche`: Flächengröße des Abschnitts (skaliert auf die amtliche Fläche); Wenn die amtliche Fläche eines Flurstücks 0 ist, wird nicht skaliert.
   `art`: Abschnittsart/Text
   `wkb_geometry`: Flächengeometrie des Abschnitts
   ggf. weitere Attribute aus den mit dem Flurstück verschnittene Tabelle
3. `faf_osf`: wie `faf_tng` jedoch für die Abschnitte nach „Öffentlich-rechtlichen und sonstigen Festlegungen (OSF)“.
4. `faf_bub`: wie `faf_tng` jedoch mit den Abschnitten nach „Bodenschätzung und Bewertung (BUB)“ und der Ertragsmesszahl (emz).
5. `faf_log`: Liste der einzelnen Verschneidungsläufe mit verschnittener Tabelle, Startzeit, Endzeit, Anzahl der erzeugten Abschnitte, Kreis (`land`, `regierungsbezirk`, `kreis`).

Vor dem Start der generierten Skripte werden alle Geometrietabellen auf ungültige Geometrie untersucht und ggf. korrigiert (die defekten Geometrien werden in einer mit der ursprünglichen korrespondierenden Tabelle mit dem Zusatz `_defekt` gesichert; die Korrektur kann mit der Option `fixareas` auch abgeschaltet werden; s.u.).

Wenn E-Mail-Adressen in der Konfiguration angegeben werden wird ein Verschneidungsbericht inkl. der Protokoll der SQL-Skripte per E-Mail verschickt. Anderenfalls und auch wenn der Versand der E-Mail fehlschlägt wird der Bericht auf der Standardausgabe ausgeben.

## Allgemeine Konfiguration
Mit „#“ eingeleitete Zeilen werden als Kommentar betrachtet und ansonsten ignoriert. Platzhalter für Benutzereinstellungen sind *kursiv* gesetzt.

`db=`*`verbindungsdaten`*

Zugangsdaten zur PostGIS-Datenbank (z.B. `dbname=alkis`). Immer notwendig.

`prefix=`*`tabellenpräfix`*

Präfix für Tabellen (Vorgabe `faf`; optional)

*`gruppe.element=beschriftung`*

Parameter zur Splissflächenunterdrückung gemäß Ihrem Konzept nach Gruppen tng, osf oder bub und Parameter p1, p2 und p3 (optional; Vorgabe `0` für P1, `5` für P2 und `0.5` für P3).

`jobs=`*`anzahl`*

Anzahl der Jobs, die Parallel ausgeführt werden sollen (korrespondiert mit `--jobs` von GNU parallel(1); optional; Vorgabe: `-1` = vorhandene Kerne – 1)

`cleanup=`*`1|0`*

Temporärverzeichnis nach der Verarbeitung abräumen (optional; Vorgabe: `1` = aktiv). Ggf. mit 0 deaktivieren, um SQL-Skripte und Protokolle zu erhalten.

`fixareas=`*`1|0`*

Ungültige Geometrien in den an der Verschneidung beteiligte Geometrietabellen korrigieren (mit PostGIS-Funktion `st_makevalid`; optional; Vorgabe `1` = aktiv). Sollte nur deaktiviert werden, wenn die Geometrien bereits korrigiert sind, anderenfalls sind Verschneidungsfehler zu erwarten.

`schema=`*`schema`*

Name des ALKIS-Schemas (Vorgabe: `public`)

`pgschema=`*`schema`*

Name des PostGIS-Schemas (Vorgabe: `postgis_21`)

`email=`*`adresse`*

Eine Adresse an die das Protokoll versendet werden soll (optiona; keine Vorgabe; Mehrfachangaben sind möglich).

## Verschneidungsregeln

### Vordefinierte Regeln

*`gruppe.tabelle=Text`*

Ordnet die Tabelle `tabelle` der Verschneidungsgruppe `gruppe` zu und legt den Text fest der erscheinen soll, wenn keine näheren Angaben geregelt wurden.

*`gruppe.tabelle1.attribut=tabelle2`*

Ordnet die Tabelle `tabelle1` der Verschneidungsgruppe `gruppe` zu und legt fest, dass Abschnitte nach dem Attribut `attribut` bezeichnet werden sollen und die Texte hierzu aus der Tabelle `tabelle2` zu entnehmen sind.

*`gruppe.tabelle1.attribut1=tabelle2.attribut2.attribut3`*

Ordnet die Tabelle `tabelle1` der Verschneidungsgruppe `gruppe` zu und legt fest, dass Abschnitte nach dem Attribut `attribut1` bezeichnet werden sollen und die Texte hierzu aus der Tabelle `tabelle2` zu entnehmen sind. In letzterer Tabelle sind die Werte aus `attribut1` in `attribut2` enthalten und der Text befindet sich in `attribut3`.
Attribut `attribut1` und `attribut2` können auch in der Form `teilattribut1|teilattribut2` angegeben werden, wenn die Klassifizierungswert zusammengesetzt werden müssen (wird bei den ausführenden Stellen in der Form `land|stelle` verwendet).

### Benutzerdefinierte Übersteuerung

*`tabelle.attribut1=wert1&attribut2=wert2|Text`*

*`tabelle1.attribut1=wert1.relation.tabelle2.attribut2=wert2|Text`*

Regeln mit zwei Attributen legen den Text für Abschnitte der Flurstücke mit den gegebenen Tabelle fest bei denen die beiden Attribute mit den Werten der Verschneidungstabelle übereinstimmen. Bei der Regelvariante mit Relation und zwei Tabellen wird erwartet, dass die erste Tabelle das Geometriefeld `wkb_geometry` sowie das gegebene Relationsfeld enthält und sich dies auf die zweite Tabelle mit dem zweiten Attribut bezieht.

*`tabelle.attribut=wert|Text`*

Regeln mit einem Attribut werden ausgewertet, wenn keine Regeln mit zwei Attributen zutreffen oder vorhanden sind. Hier können je Abschnitt mehrere Regeln zutreffen. In diesem Fall werden die Texte der zutreffenden Regeln in der Reihenfolge ihrer Angabe in der Konfiguration durch Semikolon getrennt kumuliert.

*`tabelle|Text`*

Regeln ohne Attribut treffen zu, wenn keine Regeln mit Attributen zutreffen oder vorhanden sind.

*`tabelle.attribut=<Attributwert>|<Attributwert>`*

Attribut das der ursprünglichen Tabelle, das in die jeweilige Abschnittstabelle übernommen werden soll (Mehrfachverwendung ist möglich). 

Treffen keine Regeln zu entfällt der Abschnitt. 

Für die Tabellen `ax_schutzgebietnachwasserrecht` und `ax_schutzgebietnachnaturumweltoderbodenschutzrecht` wird automatisch die Tabelle `ax_schutzzone` für die Geometrie einbezogen, wenn sie nicht bereits durch die Relationsvariante angegeben sind.


Lizenz: [GNU GPLv2](https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html)
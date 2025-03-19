#!/usr/bin/perl
############################################################################
#
# Project:  Flurstücksabschnittsverschneider
# Author:   Jürgen E. Fischer <jef@norbit.de>
# Stand:    44d46a9 2018-06-19 23:01:05 +0200
#
############################################################################
# Copyright (c) 2017, 2018, Jürgen E. Fischer <jef@norbit.de>
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 2 of the License, or
#   (at your option) any later version.
#
############################################################################

BEGIN {
	$ENV{PGCLIENTENCODING} = "UTF-8";
}

use strict;
use warnings;
use utf8;
use File::Temp qw/tempdir tempfile/;
use MIME::Lite;
use Encode;
use DBI;
use open qw/:std :utf8/;

my $db;
my $prefix = "faf";
my $jobs = "-1";
my $cleanup = 1;
my $fixareas = 1;
my $schema = "public";
my $pgschema = "postgis";
my @email;
my $dbh;

my $JOBDIR;
my $JOB;
my $JOBNAME;
my $NJOBS;
my $init;
sub newjob($$) {
	my ($name, $group) = @_;

	if(defined $JOB) {
		if($JOBNAME ne 'init') {
			print $JOB "\nRETURNING 1\n)\n";
			print $JOB "UPDATE ${prefix}_log SET n=(select count(*) from rows) WHERE table_name='$JOBNAME' AND land=:'land' AND regierungsbezirk=:'regierungsbezirk' AND kreis=:'kreis';\n";
			print $JOB "UPDATE ${prefix}_log SET endtime=now() WHERE table_name='$JOBNAME' AND land=:'land' AND regierungsbezirk=:'regierungsbezirk' AND kreis=:'kreis';\n";
		}

		print $JOB "SELECT now();\n";
		close $JOB;
	}

	return undef unless defined $name;

	$JOBDIR = tempdir(CLEANUP => $cleanup) unless defined $JOBDIR;

	my $filename = "$JOBDIR/$name.sql";
	
	open $JOB, ">$filename" or die "Konnte $filename nicht erzeugen: $!";
	$JOBNAME = $name;

	my $appname;
	if($JOBNAME eq 'init') {
		$appname = "'FAF - Initialisierung'";
	} else {
		$appname = "concat('FAF - " . (defined $group ? "$group:" : "") . "$name - Kreis ', :'land', :'regierungsbezirk', :'kreis')";
		$NJOBS++;
	}

	print $JOB <<EOF;
\\timing on
\\set ON_ERROR_STOP on
\\set ECHO queries

SELECT set_config('application_name',$appname,false);

SET search_path = $schema, public, $pgschema;

SELECT now();

EOF

	unless(defined $init) {
		$init = $filename;
	} else {
		print $JOB "INSERT INTO ${prefix}_log(table_name,starttime,gruppe,land,regierungsbezirk,kreis) VALUES ('$name',now(),'$group',:'land',:'regierungsbezirk',:'kreis');\n\n";
		print $JOB "WITH rows AS (\n";

		open F, ">>$JOBDIR/job.lst";
		print F "$filename\n";
		close F;
	}

	return $JOB;
}

sub clean($) {
	my $t = shift;

	$t = lc $t;
	$t =~ s/ß/ss/g;
	$t =~ s/ä/ae/g;
	$t =~ s/ö/oe/g;
	$t =~ s/ü/ue/g;

	return $t;
}

sub expr($$$) {
	my($v, $t, $f) = @_;

	return "$v=ANY($f)" if "$t.$f" =~ /^(ax_bahnverkehr\.bahnkategorie|ax_bodenschaetzung\.entstehungsartoderklimastufewasserverhaeltnisse|ax_bodenschaetzung\.sonstigeangaben|ax_bodenschaetzung\.entstehungsart)$/;

	return "$f=$v";
}

my %parameter = (
	'tng' => { 'p1' => 0.0, 'p2' => 5.0, 'p3' => 0.5 },
	'osf' => { 'p1' => 0.0, 'p2' => 5.0, 'p3' => 0.5 },
	'bub' => { 'p1' => 0.0, 'p2' => 5.0, 'p3' => 0.5 },
);

my %group;
my %groupattr;
my %table;

#
# tablename => {
#	'cond0' => Label
#	'cond1' => "field=value" => Label
#	'cond2' => "field1=value1 AND field2=value2" => Label
#	'fields' => "field.attribute" => 1
#	'geom' => table.fieldname
#	'join' => table ON ...
#	'gml_id' => "field"
# }
#

while(<>) {
	next if /^#|^\s*$/;

	chomp;

	my($arg,$label) = split /\|/;

	my ($t, $t0, $f0, $v0, $t1, $f1, $v1, $r);

	#
	# Parameter
	# 

	# Splissflächenparameter
	if( /^(tng|osf|bub)\.(p[123])=(\d*(.\d*)?)$/ ) {
		die "$1.$2: empty parameter" if $3 eq "";
		$parameter{$1}{$2} = $3;
		next;

	# Vorgabebeschriftung (und Gruppenzuordnung)
	# tng.AX_Wohnbauflaeche=Wohnbaufläche
	} elsif(/^(tng|osf|bub)\.([^.]+)=(.*)$/ ) {
		$t = clean($2);
		$group{$t} = $1;
		$table{$t}{cond0} = $3;

	# tng.AX_IndustrieUndGewerbeflaeche.funktion=ax_funktion_industrieundgewerbeflaeche
	# osf.AX_KlassifizierungNachStrassenrecht.land|stelle=ax_dienststelle.land|stelle.bezeichnung
	} elsif(/^(tng|osf|bub)\.([^.=]+)\.([^.=]+)=(.+)$/ ) {
		$t = clean($2);
		$f0 = clean($3);
		$t1 = $4;

		$group{$t} = $1;

		my($prepend) = $t1 =~ /&prepend=([^&]+)/;
		my($append) = $t1 =~ /&append=([^&]+)/;
		my($attributes) = $t1 =~ /&attributes=([^&]+)/;

		if(defined $attributes) {
			for (split /,/, $attributes) {
				$table{$t}{fields}{$_} = 1;
			}
		}

		$t1 =~ s/&.*$//;
		$t1 = clean($t1);

		if($f0 =~ /\|/) {
			my $alias = $dbh->quote_identifier($f0);
			my $expr = join("||", map { "$t.$_"; } split /\|/, $f0);
			$table{$t}{fields}{"$expr AS $alias"} = 1;
			$f0 = $alias;
		} else {
			$table{$t}{fields}{$f0} = 1;
		}

		my $fk = "wert";
		my $fv = "beschreibung";

		if($t1 =~ /^([^.]+)\.([^.]+)\.([^.]+)$/ ) {
			$t1 = clean($1);
			$fk = clean($2);
			$fv = clean($3);

			$fk =~ s/\|/\|\|/g;
			$fv =~ s/\|/\|\|/g;
		}

		my $sth = $dbh->prepare( "SELECT $fk,$fv FROM $t1 ORDER BY $fk" );
		$sth->execute;
		while( my($k,$v) = $sth->fetchrow_array ) {
			$v = $k unless defined $k;

			$k = $dbh->quote($k) if $fk =~ /\|\|/;

			$arg = expr($k, $t, $f0);

			push @{ $table{$t}{cond1}{order} }, $arg unless exists $table{$t}{cond1}{$arg};
			$v = $prepend . "||" . $dbh->quote($v) if $prepend;
			$v = $dbh->quote($v) . "||" . $append if $append;
			$table{$t}{cond1}{arg}{$arg} = $v;
		}

	# Tabellenpräfix
	} elsif( /^prefix=(\S+)/ ) {
		$prefix = $1;
		next;

	# Datenbank
	} elsif( /^db=(.*)$/ ) {
		$db = $1;

		$dbh = DBI->connect("dbi:Pg:$db", undef, undef, { RaiseError => 1, pg_enable_utf8 => -1, });
		die "Datenbankverbindung gescheitert." . DBI->errstr unless defined $dbh;

		next;

	# Parallele Jobs
	} elsif( /^jobs=(\d+)$/ ) {
		$jobs = $1;
		next;

	# Temporäres Jobverzeichnis abräumen (default ja)
	} elsif( /^cleanup=([01])$/ ) {
		$cleanup = $1;
		next;

	# Defekte Geometrien reparieren
	} elsif( /^fixareas=([01])$/ ) {
		$fixareas = $1;
		next;

	# ALKIS-Schema
	} elsif( /^schema=(.*)$/ ) {
		$schema = $1;
		next;

	# PostGIS-Schema
	} elsif( /^pgschema=(.*)$/ ) {
		$pgschema=$1;
		next;

	# E-Mail-Adressen
	} elsif( /^email=(.*)$/ ) {
		push @email, $1;
		next;

	#
	# Regeln
	#	

	# Regel catch all 
	# AX_Wohnbauflaeche
	} elsif( ($t0) = $arg =~ /^([^=.]+)$/ ) {
		$t = clean($t0);

		$table{$t}{cond0} = $label;

	# Regel mit einer Bedingung
	# AX_IndustrieUndGewerbeflaeche.funktion=1400
	} elsif( ($t0, $f0, $v0) = $arg =~ /^([^.]+)\.([^=]+)=(\d+)$/ ) {
		$t = clean($t0);
		$f0 = clean($f0);

		$arg = expr($v0, $t, $f0);

		push @{ $table{$t}{cond1}{order} }, $arg unless exists $table{$t}{cond1}{arg}{$arg};
		$table{$t}{cond1}{arg}{$arg} = $label;

		$table{$t}{fields}{$f0} = 1;

	# Regel mit zwei Bedingungen
	# AX_IndustrieUndGewerbeflaeche.funktion=2510&foerdergut=1000
	} elsif( ($t0, $f0, $v0, $f1, $v1) = $arg =~ /^([^.]+)\.([^=]+)=(\d+)&([^=]+)=(\d+)$/ ) {
		$t = clean($t0);
		$f0 = clean($f0);
		$f1 = clean($f1);

		$arg = "(" . expr($v0, $t, $f0) . " AND " . expr($v1, $t, $f1) . ")";

		push @{ $table{$t}{cond2} }, [$arg, $label];

		$table{$t}{fields}{$f0} = 1;
		$table{$t}{fields}{$f1} = 1;

	# Regel mit zwei Bedingungen und Join
	# AX_Schutzzone.zone=1010.istTeilVon.AX_SchutzgebietNachWasserrecht.artDerFestlegung=1510
	} elsif( ($t0, $f0, $v0, $r, $t1, $f1, $v1) = $arg =~ /^([^.]+)\.([^=]+)=(\d+)\.([^.]+)\.([^.]+)\.([^=]+)=(\d+)$/ ) {
		$t0 = clean($t0);
		$f0 = clean($f0);
		$t1 = clean($t1);
		$f1 = clean($f1);
		$r = clean($r);

		$arg = "(" . expr($v0, $t0, $f0) . " AND " .expr($v1, $t1, $f1) . ")";

		$t = $t1;

		$table{$t}{table} = $t0;
		$table{$t}{geom} = "$t0.wkb_geometry";
		$table{$t}{join} = "$t1 ON $t1.endet IS NULL AND ARRAY[$t1.gml_id] <@ $t0.$r";
		$table{$t}{gml_id} = "$t1.gml_id||':ax_schutzzone '||$t0.gml_id";

		# TODO: Qualifizieren, falls Felder in beiden Tabellen vorkommen
		$table{$t}{fields}{$f0} = 1;
		$table{$t}{fields}{$f1} = 1;

		push @{ $table{$t}{cond2} }, [$arg, $label];

	# Zusätzliche Attribute
	# AX_Bodenschaetzung.bodenzahlOderGruenlandgrundzahl=<Attributwert>
	} elsif( ($t0, $f0) = $arg =~ /^([^.]+)\.([^=]+)=<Attributwert>$/ ) {
		die "<Attributwert> statt $label erwartet" unless $label eq "<Attributwert>";
	
		$t0 = clean($t0);
		$f0 = clean($f0);

		push @{ $table{$t0}{attrs} }, $f0;

		$groupattr{$group{$t0}}{$f0} = 1;

		next;

	} else {
		die "Ungültige Zeile: $arg";

	}

	# ax_schutzzone ggf. mit einbeziehen
	if($t =~ /^(ax_schutzgebietnachwasserrecht|ax_schutzgebietnachnaturumweltoderbodenschutzrecht)$/ && !exists $table{$t}{join} ) {
		$table{$t}{table} = "ax_schutzzone";
		$table{$t}{geom} = "ax_schutzzone.wkb_geometry";
		$table{$t}{join} = "$t ON $t.endet IS NULL AND ARRAY[$t.gml_id] <@ ax_schutzzone.istteilvon";
		$table{$t}{gml_id} = "$t.gml_id||':ax_schutzzone '||ax_schutzzone.gml_id";
	}

	$table{$t}{table} = "$t" unless exists $table{$t}{table};
	$table{$t}{gml_id} = "$t.gml_id" unless exists $table{$t}{gml_id};
	$table{$t}{geom} = "$t.wkb_geometry" unless exists $table{$t}{geom};
}

die "Keine Datenbankverbindung aufgenommen." unless defined $dbh;

my $o = newjob("init", undef);

print $o <<EOF;
CREATE OR REPLACE FUNCTION faf_intersection(fs GEOMETRY, g GEOMETRY, f double precision, error TEXT, p1 numeric = 0.0, p2 numeric = 5.0, p3 numeric = 0.5) RETURNS GEOMETRY[] AS \$\$
DECLARE
	a numeric;
	r RECORD;
	res GEOMETRY[];
	-- PN 09-12-2022: org und intersection dienen dem Vergleich Flurstück == Splissfläche
	org numeric;
	intersection numeric;
BEGIN
	IF NOT fs && g THEN
		RETURN NULL;
	END IF;

	IF f=0 THEN
		f := NULLIF(st_area(fs), 0);
		RAISE NOTICE 'Amtliche Flurstücksfläche 0 durch geometrische Fläche % ersetzt bei %', f, error;
	END IF;

	org := st_area(fs);
	intersection := st_area(st_intersection(fs,g));

	FOR r IN SELECT geom FROM st_dump(st_intersection(fs,g))
	LOOP
		IF geometrytype(r.geom) <> 'POLYGON' THEN
			CONTINUE;
		END IF;

		-- PN 09-12-2022: Wenn Flurstück und Splissfläche gleich groß, dann handelt es sich um ein kleines Flurstück, welches vollständig weiter verarbeitet werden soll.
		IF org=intersection THEN
			res := array_append(res, r.geom);
			CONTINUE;
		END IF;

		a := st_area(r.geom);
		IF p3=0 THEN
			IF f>=p1 AND a<=p2 THEN
				CONTINUE;		
			END IF;
		ELSIF p3>0 THEN
			IF a<p2 AND a/f<p3 THEN
				CONTINUE;
			END IF;
		END IF;

		res := array_append(res, r.geom);
	END LOOP;

	RETURN res;
EXCEPTION WHEN OTHERS THEN
	RAISE NOTICE 'faf_intersection-Ausnahme bei: %: %', error, SQLERRM;
	RETURN NULL;
END;
\$\$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION faf_fsschwerpunkt(g GEOMETRY, gml_id varchar) RETURNS GEOMETRY AS \$\$
BEGIN
	RETURN st_pointonsurface(g);
EXCEPTION WHEN OTHERS THEN
	BEGIN
		RETURN st_centroid(g);
	EXCEPTION WHEN OTHERS THEN
		RAISE NOTICE 'Kein Schwerpunkt für ax_flurstueck %', gml_id;
		RETURN NULL;
	END;
END;
\$\$ LANGUAGE plpgsql;

CREATE FUNCTION pg_temp.indexonkreis() RETURNS VOID AS \$\$
BEGIN
	CREATE INDEX faf_kreis_idx ON ax_flurstueck(gemeindezugehoerigkeit_land,gemeindezugehoerigkeit_regierungsbezirk,gemeindezugehoerigkeit_kreis,endet);
EXCEPTION WHEN OTHERS THEN
        NULL;
END;
\$\$ LANGUAGE plpgsql;

SELECT pg_temp.indexonkreis();

CREATE OR REPLACE FUNCTION faf_toint(v anyelement) RETURNS integer AS \$\$
BEGIN
        RETURN v::int;
EXCEPTION WHEN OTHERS THEN
        RETURN NULL;
END;
\$\$ LANGUAGE plpgsql IMMUTABLE STRICT;

DROP TABLE IF EXISTS ${prefix}_log;
CREATE TABLE ${prefix}_log(
	table_name varchar, 
	gruppe varchar,
	starttime timestamp,
	endtime timestamp,
	n integer,
	land varchar,
	regierungsbezirk varchar,
	kreis varchar
);

CREATE INDEX ${prefix}_log_idx ON ${prefix}_log(gruppe,land,regierungsbezirk,kreis);

DROP TABLE IF EXISTS ${prefix}_fs;
CREATE TABLE ${prefix}_fs(
	gml_id character(16) NOT NULL PRIMARY KEY,
	flurstueckskennzeichen varchar NOT NULL,
  	amtlicheflaeche double precision NOT NULL,
  	geometrischeflaeche double precision NOT NULL,
	gmz double precision,
	schwerpunkt geometry(point,25832)
);

CREATE UNIQUE INDEX ${prefix}_fs_gml_id_idx ON ${prefix}_fs(gml_id);
CREATE UNIQUE INDEX ${prefix}_fs_fskz_idx ON ${prefix}_fs(flurstueckskennzeichen);
CREATE INDEX ${prefix}_schwerpunkt_idx ON ${prefix}_fs USING GIST(schwerpunkt);

EOF

for my $group (qw/tng osf bub/) {
	print $o <<EOF;
DROP TABLE IF EXISTS ${prefix}_$group;
CREATE TABLE ${prefix}_$group(
	ogc_fid SERIAL PRIMARY KEY,
	gml_id character(16) not null,
	flaeche double precision,
	art varchar,
	wkb_geometry geometry(polygon,25832)
EOF

	print $o ",\n\t" . join(",\n\t", map { "$_ varchar" } keys %{ $groupattr{$group} } ) if exists $groupattr{$group};

	print $o ",\n\temz double precision\n" if $group eq "bub";

	print $o <<EOF;
);

CREATE INDEX ${prefix}_${group}_gml_id_idx ON ${prefix}_${group}(gml_id);
CREATE INDEX ${prefix}_${group}_wkb_geometry_idx ON ${prefix}_${group} USING GIST(wkb_geometry);

EOF
}

my @fixareas;
if($fixareas) {
	print $o <<EOF;
CREATE OR REPLACE FUNCTION faf_fixareas(t TEXT) RETURNS VARCHAR AS \$\$
DECLARE
	n INTEGER;
	m TEXT;
BEGIN
	BEGIN
		EXECUTE 'SELECT count(*) FROM ' || t || ' WHERE NOT st_isvalid(wkb_geometry)' INTO n;
		IF n = 0 THEN
			RETURN 'Keine ungültigen Geometrien in ' || t;
		END IF;

		BEGIN
			EXECUTE 'CREATE TABLE ' || t || '_defekt AS SELECT gml_id,beginnt,wkb_geometry FROM ' || t || ' WHERE NOT st_isvalid(wkb_geometry) OR geometrytype(wkb_geometry)=''GEOMETRYCOLLECTION''';
		EXCEPTION WHEN OTHERS THEN
			EXECUTE 'INSERT INTO ' || t || '_defekt(gml_id,beginnt,wkb_geometry) SELECT gml_id,beginnt,wkb_geometry FROM ' || t || ' WHERE NOT st_isvalid(wkb_geometry) OR geometrytype(wkb_geometry)=''GEOMETRYCOLLECTION''';
		END;

		EXECUTE 'UPDATE ' || t || ' SET wkb_geometry=st_collectionextract(st_makevalid(wkb_geometry),3) WHERE NOT st_isvalid(wkb_geometry) OR geometrytype(wkb_geometry)=''GEOMETRYCOLLECTION''';
		GET DIAGNOSTICS n = ROW_COUNT;
		IF n > 0 THEN
			RAISE NOTICE '% Geometrien in % korrigiert.', n, t;
		END IF;

		RETURN t || ' geprüft (' || n || ' ungültige Geometrien in ' || t || '_defekt gesichert und korrigiert).';
	EXCEPTION WHEN OTHERS THEN
		m := SQLERRM;

		BEGIN
			EXECUTE 'SELECT count(*) FROM ' || t || ' WHERE NOT st_isvalid(wkb_geometry) OR geometrytype(wkb_geometry)=''GEOMETRYCOLLECTION''' INTO n;
			IF n > 0 THEN
				RAISE EXCEPTION '% defekte Geometrien in % gefunden - Ausnahme bei Korrektur: %', n, t, m;
			END IF;
		EXCEPTION WHEN OTHERS THEN
			RAISE EXCEPTION 'Ausnahme bei Bestimmung defekter Geometrien in %: %', t, SQLERRM;
		END;

		RETURN 'Ausnahme bei Korrektur: '||SQLERRM;
	END;
END;
\$\$ LANGUAGE plpgsql;

EOF

	my %geom;
	for my $t (keys %table) {
		my $geom = $table{$t}{geom};
		$geom =~ s/\.wkb_geometry$//;
		$geom{$geom} = 1;
	}

	@fixareas = keys %geom;
	push @fixareas, "ax_flurstueck";

	print $o "SELECT faf_fixareas(t)\nFROM unnest(ARRAY[\n\t"
		. join(",\n\t", map { $dbh->quote($_); } @fixareas)
		. "\n]) AS t;\n\n";
}

$o = newjob("ax_flurstueck", 'fs');

print $o <<EOF;
INSERT INTO ${prefix}_fs(gml_id,flurstueckskennzeichen,amtlicheflaeche,geometrischeflaeche,schwerpunkt) 
SELECT
	gml_id,
	flurstueckskennzeichen,
	amtlicheflaeche,
	round(st_area(wkb_geometry)::numeric) AS geometrischeflaeche,
	faf_fsschwerpunkt(wkb_geometry, gml_id) AS schwerpunkt
FROM ax_flurstueck
WHERE endet IS NULL
  AND ax_flurstueck.gemeindezugehoerigkeit_land=:'land' AND ax_flurstueck.gemeindezugehoerigkeit_regierungsbezirk=:'regierungsbezirk' AND ax_flurstueck.gemeindezugehoerigkeit_kreis=:'kreis'
EOF

for my $t (keys %table) {
	my $group = $group{$t};
	die "Gruppe für $t nicht gefunden." unless defined $group;

	my $p1 = $parameter{$group}{p1};
	my $p2 = $parameter{$group}{p2};
	my $p3 = $parameter{$group}{p3};

	$o = newjob($t, $group);

	my @cols = qw/gml_id art wkb_geometry/;
	push @cols, @{ $table{$t}{attrs} } if exists $table{$t}{attrs};

	print $o "INSERT INTO ${prefix}_$group(" . join(",", @cols);
	print $o ",flaeche";
	print $o ",emz" if $t eq "ax_bodenschaetzung";
	print $o ")\n";
	print $o "SELECT " . join(",", @cols);
	print $o ",round(flaeche::numeric) AS flaeche";
	print $o ",round(round(flaeche::numeric)*we2/100.0) AS emz" if $t eq "ax_bodenschaetzung";
	print $o " FROM (SELECT\n";
	print $o "\tgml_id,\n";
	print $o "\tst_area(unnest(ngeom))*a/st_area(fgeom) AS flaeche,\n";
	
	my @where;
	my @cases;

	if(exists $table{$t}{cond2}) {
		for my $c (@{ $table{$t}{cond2} }) {
			push @cases, "WHEN " . $c->[0] . " THEN " . $dbh->quote($c->[1]);
			push @where, $c->[0];
		}
	}

	my $case;
	if(exists $table{$t}{cond1}) {
		my @arg;
		for my $c (@{ $table{$t}{cond1}{order} }) {
			my $label = $table{$t}{cond1}{arg}{$c};
			$label = $dbh->quote($label) unless $label =~ /^'.*'$/;
			push @arg, "CASE WHEN " . $c . " THEN ARRAY[" . $label . "] ELSE NULL END";
			push @where, $c;
		}

		$case .= "array_to_string(\n\t\t" . join(" ||\n\t\t", @arg) . ",\n\t\t'; '\n\t)";

		if(@cases) {
			$case =~ s/\n/\n\t/g;
			push @cases, "ELSE\n\t\t$case";
		}
	}

	$case = "CASE\n\t" . join("\n\t", @cases) . "\n\tEND" if @cases;
	
	if(exists $table{$t}{cond0}) {
		if(defined $case) {
			$case =~ s/\n/\n\t/g;
			$case = "coalesce(\n\t\t$case,\n\t\t'$table{$t}{cond0}'\n\t)";
		} else {
			$case = $dbh->quote($table{$t}{cond0});
		}
		undef @where;
	}

	unless(defined $case) {
		die "no case";
	}

	print $o "\t$case AS art,\n";

	print $o "\tunnest(ngeom) AS wkb_geometry";

	print $o ",\n\t" . join(",\n\t", @{ $table{$t}{attrs} }) if exists $table{$t}{attrs};

	print $o ",\n\twe2" if $t eq "ax_bodenschaetzung";

	print $o "\nFROM (\n";
	print $o "\tSELECT\n";
	print $o "\t\tax_flurstueck.gml_id,\n";
	print $o "\t\tax_flurstueck.amtlicheflaeche AS a,\n";
	print $o "\t\tax_flurstueck.wkb_geometry AS fgeom,\n";

	foreach my $f (keys %{ $table{$t}{fields} }) {
		print $o "\t\t$f,\n";
	}

	print $o "\t\tfaf_intersection(ax_flurstueck.wkb_geometry, $table{$t}{geom}, ax_flurstueck.amtlicheflaeche, 'ax_flurstueck '||ax_flurstueck.gml_id||' <=> $t '||$table{$t}{gml_id}, $p1, $p2, $p3) AS ngeom";
	print $o ",\n\t\t" . join(",\n\t\t", @{ $table{$t}{attrs} }) if exists $table{$t}{attrs};
	print $o ",\n\t\tfaf_toint(ackerzahlodergruenlandzahl) AS we2" if $t eq "ax_bodenschaetzung";
	print $o "\n\tFROM ax_flurstueck\n";
	print $o "\tJOIN " . $table{$t}{table} . " ON ax_flurstueck.wkb_geometry && " . $table{$t}{geom} . " AND " . $table{$t}{table} . ".endet IS NULL\n";
	print $o "\tJOIN " . $table{$t}{join} . "\n" if exists $table{$t}{join};
	print $o "\tWHERE ax_flurstueck.endet IS NULL AND ax_flurstueck.gemeindezugehoerigkeit_land=:'land' AND ax_flurstueck.gemeindezugehoerigkeit_regierungsbezirk=:'regierungsbezirk' AND ax_flurstueck.gemeindezugehoerigkeit_kreis=:'kreis'";
	print $o "\n) AS foo";

	@where = ("(" . join(" OR ", @where) . ")") if @where;
	push @where, "ngeom IS NOT NULL";
	
	print $o "\nWHERE " . join(" AND ", @where) if @where;
	print $o "\n) AS foo\n";
	print $o "WHERE art IS NOT NULL";
}


newjob(undef, undef);

my $sth = $dbh->prepare(
	"SELECT"
	. " gemeindezugehoerigkeit_land"
	. ",gemeindezugehoerigkeit_regierungsbezirk"
	. ",gemeindezugehoerigkeit_kreis"
	. ",coalesce("
	.  "(SELECT bezeichnung FROM ax_kreisregion r WHERE gemeindezugehoerigkeit_land=r.land AND gemeindezugehoerigkeit_regierungsbezirk=r.regierungsbezirk AND gemeindezugehoerigkeit_kreis=r.kreis)"
	.  ",'(Kreis '||gemeindezugehoerigkeit_land||gemeindezugehoerigkeit_regierungsbezirk||gemeindezugehoerigkeit_kreis||')'"
	. ") AS name"
	. " FROM ax_flurstueck f"
	. " WHERE endet IS NULL"
	. " GROUP BY gemeindezugehoerigkeit_land,gemeindezugehoerigkeit_regierungsbezirk,gemeindezugehoerigkeit_kreis"
);

$sth->execute;

my %kreis;
my $kn = 0;
open F, ">$JOBDIR/kreise.lst";
while( my($l,$r,$k,$n) = $sth->fetchrow_array ) {
	print F "-v land=$l -v regierungsbezirk=$r -v kreis=$k\n";
	$kreis{$l}{$r}{$k} = "$n ($l$r$k)";
	$kn++;
}
close F;

$sth->finish;
$dbh->disconnect;

system "exec >$JOBDIR/faf.log 2>&1; psql '$db' -X -f '$init' && parallel --jobs=$jobs eval psql \\\\\\'$db\\\\\\' -X -f :::: $JOBDIR/job.lst :::: $JOBDIR/kreise.lst";
my $r = $?;

open F, ">$JOBDIR/report.txt";

print F "Flurstücksabschnittverschneidung\n\n";

$dbh = DBI->connect("dbi:Pg:$db", undef, undef, { RaiseError => 1, pg_enable_utf8 => -1 });
die "Datenbankverbindung gescheitert." . DBI->errstr unless defined $dbh;

$dbh->do("SET search_path TO $schema, public, $pgschema");

$dbh->do("UPDATE ${prefix}_fs a SET gmz=(SELECT sum(emz) FROM ${prefix}_bub b WHERE a.gml_id=b.gml_id)" );

my $nplaned = $NJOBS * $kn;

my ($s, $e, $d, $nrecorded, $sn) = $dbh->selectrow_array( "SELECT min(starttime),max(endtime),max(endtime)-min(starttime),count(*) AS n,sum(n) AS sn FROM ${prefix}_log" );
$s = "(unbekannt)" unless defined $s;
$e = "(unbekannt)" unless defined $e;
$d = "(unbekannt)" unless defined $d;
$nrecorded = 0 unless defined $nrecorded;
$sn = 0 unless defined $sn;

my($nincomplete) = $dbh->selectrow_array( "SELECT coalesce(count(*),0) FROM ${prefix}_log WHERE endtime IS NULL" );
my $ncompleted = $nrecorded - $nincomplete;

print F "Nur $nrecorded von $nplaned Verschneidungen durchgeführt.\n\n" if $nrecorded < $nplaned;

if($nincomplete > 0) {
	$sth = $dbh->prepare("SELECT gruppe,table_name,land,regierungsbezirk,kreis,starttime FROM ${prefix}_log WHERE endtime IS NULL ORDER BY gruppe,table_name,land,regierungsbezirk,kreis,starttime");
	$sth->execute;

	printf F "$nincomplete Verschneidungen nicht vollendet:\n\n\t%-10s %-40s %-30s %s\n", "Gruppe", "Tabelle", "Startzeit", "Kreis";
	while( my($g,$n,$l,$r,$k,$t) = $sth->fetchrow_array ) {
		printf F "\t%-10s %-40s %-30s %s\n", $g, $n, $t, $kreis{$l}{$r}{$k};
	}

	if(-f "$JOBDIR/faf.log" ) {
		print F "\nFehlermeldungen:\n";
		open I, "(egrep -A2 -B4 '(ERROR|FEHLER):' $JOBDIR/faf.log || echo Keine.) |";
		while(<I>) {
			print F;
		}
		close I;
	} else {
		print F "Protokolldatei fehlt.\n";
	}

	print F "\n";
}


print F <<EOF;

Laufzeit:
	Beginn:			$s
	Ende:			$e
	Dauer:			$d
	Abschnitte:		$sn

Verschneidungen:
	Einzelverschneidungen:	$NJOBS
	Landkreise:		$kn
	Geplante Jobs:		$nplaned
	Gestartete Jobs:	$nrecorded
	Abgeschlossene Jobs:	$ncompleted
	Unvollendete Jobs:	$nincomplete
EOF

if(@fixareas) {
	my %fixedareas;

	$sth = $dbh->prepare(
		"SELECT table_name FROM information_schema.tables WHERE table_schema='$schema' AND table_name IN ("
		. join(",", map { $dbh->quote($_ . "_defekt"); } @fixareas)
		. ")"
	);

	$sth->execute;
	while( my($t) = $sth->fetchrow_array ) {
		my($n) = $dbh->selectrow_array( "SELECT count(*) FROM ${t}" );
		$fixedareas{$t} = $n if $n>0;
	}

	if(keys %fixedareas) {
		printf F "\n\nKorrigierte Geometrien\n\n\t%-10s %s\n", "Anzahl", "Tabelle";

		for my $t (reverse sort { $fixedareas{$a} <=> $fixedareas{$b} } keys %fixedareas) {
			next unless exists $fixedareas{$t};
			printf F "\t%-10d %s\n", $fixedareas{$t}, $t;
		}
	}
}

$sth = $dbh->prepare( "SELECT coalesce(gruppe,'fs'),sum(endtime-starttime),sum(n) FROM ${prefix}_log GROUP BY gruppe ORDER BY sum(endtime-starttime) DESC" );
$sth->execute;

printf F "\n\nLaufzeit nach Gruppen\n\n\t%-16s %10s %s\n", "Dauer", "Abschnitte", "Gruppe";
while( my($g,$d,$n) = $sth->fetchrow_array ) {
	printf F "\t%-16s %10d %s\n", $d, $n, $g;
}

$sth->finish;

my($fs_amtliche, $fs_geom) = $dbh->selectrow_array( "SELECT sum(amtlicheflaeche),sum(geometrischeflaeche) FROM ${prefix}_fs" );
my($tng_amtliche, $tng_geom) = $dbh->selectrow_array( "SELECT sum(flaeche),sum(st_area(wkb_geometry)) FROM ${prefix}_tng" );

printf F "\n\nFlächensummenvergleich\n\n";
printf F "%20s %20s %20s %s\n", "Amtlich", "Geometrisch", "Vergleich [%]", "Gruppe";
printf F "%20.1f %20.1f %20.3f %s\n", $fs_amtliche, $fs_geom, $fs_amtliche/$fs_geom*100, "Flurstücke [m²]";
printf F "%20.1f %20.1f %20.3f %s\n", $tng_amtliche, $tng_geom, $tng_amtliche/$tng_geom*100, "TNG [m²]";
printf F "%20.3f %20.3f %20s\n", $tng_amtliche/$fs_amtliche*100.0, $tng_geom/$fs_geom*100.0, "Vergleich [%]";

$sth = $dbh->prepare( "SELECT land,regierungsbezirk,kreis,sum(endtime-starttime),sum(n) FROM ${prefix}_log GROUP BY land,regierungsbezirk,kreis ORDER BY sum(endtime-starttime) DESC" );
$sth->execute;

printf F "\n\nLaufzeit nach Kreis\n\n\t%-16s %-10s %s\n", "Dauer", "Abschnitte", "Kreis";
while( my($l,$r,$k,$d,$n) = $sth->fetchrow_array ) {
	printf F "\t%-16s %10d %s\n", $d, $n, $kreis{$l}{$r}{$k};
}

$sth->finish;

print F "\nJob-Verzeichnis: $JOBDIR\n" unless $cleanup;

close F;

if(@email) {
	my $msg = MIME::Lite->new(
		"From" => "faf",
		"To" => join(",", @email),
		"Subject" => encode('MIME-Header', "Flurstücksverschneidung"),
	 	"Type" => "text/plain; charset=UTF-8",
		"Path" => "$JOBDIR/report.txt"
	);

	if(-f "$JOBDIR/faf.log") {
		$msg->attach(
			"Type" => "application/gzip",
			"Path" => "gzip -c $JOBDIR/faf.log |",
			"Encoding" => "base64",
			"Filename" => "faf.log.gz",
			"Disposition" => "attachment"
		);
	}

	if( $msg->send() ) {
		exit;
	}

	print "E-Mailversand gescheitert.\n";
	system "cat $JOBDIR/report.txt";
}

if(-f "$JOBDIR/faf.log") {
	print "\nVollständiges Protokoll:\n";
	system "cat $JOBDIR/faf.log";
} else {
	print "\nProtokolldatei fehlt.";
}

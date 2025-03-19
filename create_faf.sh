#!/bin/bash

############################################################################
#
# Project:  Aufruf Flurstücksabschnittsverschneider
# Author:   Oliver Schmidt, LVermGeo RP
# Stand:    2025-03-19
#
############################################################################
# Copyright (c) 2025, LVermGeo RP
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 2 of the License, or
#   (at your option) any later version.
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
############################################################################

################
# Definitionen #
################

dbname="alkis"
dbport="5432"
dbuser="import"
dbhost="localhost"

#######################################
# bereits vorhandene Tabellen löschen #
#######################################

psql -c "DROP TABLE IF EXISTS faf_bub;" -d $dbname -p $dbport -U $dbuser
psql -c "DROP TABLE IF EXISTS faf_fs;" -d $dbname -p $dbport -U $dbuser
psql -c "DROP TABLE IF EXISTS faf_osf;" -d $dbname -p $dbport -U $dbuser
psql -c "DROP TABLE IF EXISTS faf_tng;" -d $dbname -p $dbport -U $dbuser

####################################################
# Flächenabschnitte der Flurstücke (FAF) berechnen #
####################################################

perl faf.pl faf.ini faf.ctl faf.txt

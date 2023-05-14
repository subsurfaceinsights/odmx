#Not needed of python3
#from __future__ import (absolute_import, division, print_function)

""" DbWrench_DDL_postprocess.py
Modified from the version created Emilio Mayorga (UW/APL) by Doug Johnson et al
8/15-18/2014
Take the DDL SQL output from DbWrench for PostgreSQL, and apply changes in order to
generate a new, blank ODMX database following ODMX conventions. Specifically:
1. All entity names will be lowercase
2. All entities will be under a single schema
3. The field samplingfeatures.featuregeometry will be PostGIS geometry field constrained
   to be 2D, but otherwise free to store any project (eg, epsg:4326) and to
   accept any geometry type (point, line, polygon, and collections thereof [multi-polygon, etc])

- Assumes that the source DDL SQL file is in the same directory as the script
- This DDL must be run on a pre-existing, "empty" database. All permissions (roles) settings
must be set up by the database administrator.
------------------------------------

Note: developed and tested on Linux only.
------------------------------------
See this great online tool (there are others), to test regex, export to code, etc
http://regex101.com

Adapted by Doug Johnson (Subsurface Insights) For working ODM2.1
9/17/2018

Adapted by Doug Johnson (Subsurface Insights) to use snake_case
2/6/2019

Adapted by Roelof Versteeg (Subsurface Insights) to be fully OXMX
3/6/2019
"""

import re
import sys

if (len(sys.argv) < 3):
    print("Usage:")
    print("    DbWrench_DDL_postprocess.py <input file name generated from DbWrench> <output file name>")
    sys.exit(1);

# =============== USER (run-time) CHANGES =================
# DDL input file name
#ddlfile = 'ODMX_DDL_for_PostgreSQL9.3PostGIS2.1.sql'
ddlfile = sys.argv[1]
# DDL output file name
#ddlppfile = 'ODMX_DDL_for_PostgreSQL9.3PostGIS2.1_postprocessed.sql'
ddlppfile = sys.argv[2]

newschemaname = 'odmx'
odmversion = 'X'
odmxinfodct = {'schema': newschemaname, 'version': odmversion}
# =========================================================


pre_block = """ /* Post-processed DDL based on DbWrench export. 2014-8-18 10pm PDT */


DROP SCHEMA IF EXISTS odmx CASCADE;
DROP SCHEMA IF EXISTS odmx_materialized_views CASCADE;

/* Add single base schema for all odmx entities */
CREATE SCHEMA %(schema)s;
COMMENT ON SCHEMA %(schema)s IS 'Schema holding all ODMX (%(version)s) entities (tables, etc).';
/* SSI SPECIFIC SCHEMA */
CREATE SCHEMA odmx_materialized_views;
COMMENT ON SCHEMA odmx_materialized_views IS 'Schema holding all SSI materialized views';

CREATE EXTENSION if not exists postgis;
/* Not sure whether these are needed
CREATE EXTENSION if not exists postgis_topology;
CREATE EXTENSION if not exists fuzzystrmatch;
CREATE EXTENSION if not exists postgis_tiger_geoCoder; */
""" % odmxinfodct

#post_block = """/* ** Set up samplingfeatures.featuregeometry as a heterogeneous, 2D PostGIS geom field. */
#ALTER TABLE %(schema)s.samplingfeatures ALTER COLUMN featuregeometry TYPE geometry;
#ALTER TABLE %(schema)s.samplingfeatures ADD CONSTRAINT
#  enforce_dims_featuregeometry CHECK (st_ndims(featuregeometry) = 2);
#CREATE INDEX idx_samplingfeature_featuregeom ON %(schema)s.samplingfeatures USING gist (featuregeometry);
#-- Populate and tweak geometry_columns
#SELECT Populate_Geometry_Columns();
#-- But it assigned a POINT type to  %(schema)s.samplingfeatures. Need instead to use the generic
#-- 'geometries', to accept any type (point, line, polygon, and collections thereof [multi-polygon, etc])
#UPDATE public.geometry_columns SET
#type = 'GEOMETRY' WHERE f_table_schema = '%(schema)s' AND f_table_name = 'samplingfeatures';
#""" % odmxinfodct
#
# Relies on these assumptions:
# 1. All schema names start with the prefix "ODMX"
# 2. No entity other than schemas starts with the prefix "ODMX"


#This function simply converst CamelCase to camel_case. We do this because camelcase flattens to lowercase
#which makes the API harder to use
def convertCamelCaseToSnake(string):
    """
    Converts a CamelCase style string to snake_case
    """
    #Taken from https://stackoverflow.com/questions/1175208/elegant-python-function-to-convert-camelcase-to-snake-case
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', string)
    conversion = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    return conversion

def convertReMatchCamelCaseToSnake(match):
    return convertCamelCaseToSnake(match.group(1))

odmx_schema_conv_re = re.compile('"(ODMX\w*?)"(?=\.)')
quoted_feature_re = re.compile('"(\w*?)"')

ddl_pp_lines = []
with open(ddlfile, 'r') as ddl_f:
    for ddl_ln in ddl_f.readlines():
        if (('add schema' in ddl_ln.lower()) or ('update: schema' in ddl_ln.lower()) or ('create schema' in ddl_ln.lower()) or ('comment on schema' in ddl_ln.lower())  or   ('drop schema public' in ddl_ln.lower()) ):
        #if ('schema' in ddl_ln.lower()):
            # Skip the line, so it won't be written out
            # Assumes all schema statements are found as single lines
            continue
        else:
            processed_line = re.sub(odmx_schema_conv_re, newschemaname, ddl_ln)
            processed_line = re.sub(quoted_feature_re, convertReMatchCamelCaseToSnake, processed_line)
            ddl_pp_lines.append(processed_line.replace("__", "_"))





# Write out new, post-processed DDL file
# Insert pre and post blocks, and the modified DDL lines in between
ddl_ppf = open(ddlppfile, 'w')
ddl_ppf.write(pre_block)
ddl_ppf.write('/* ================================================================\n')
ddl_ppf.write('   ================================================================ */\n\n')
ddl_ppf.writelines(ddl_pp_lines)
ddl_ppf.write('\n/* ================================================================\n')
ddl_ppf.write('   ================================================================ */\n\n')
#ddl_ppf.write(post_block)
ddl_ppf.close()

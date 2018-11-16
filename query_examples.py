import json
import tornado.websocket


class BaseHandler(tornado.web.RequestHandler):
    def get_current_user(self):
        return self.get_secure_cookie("usera")


class MyExamplesHandler(BaseHandler):
    @tornado.web.authenticated
    def get(self):
        query0 = """--
-- Example Query --
-- This query selects 0.001% of the data
SELECT RA, DEC, MAG_AUTO_G, TILENAME from Y3_GOLD_2_2 sample(0.001)
"""
        query0ra = """--
-- Example Query --
-- This query selects the first 1000 rows from a RA/DEC region
SELECT ALPHAWIN_J2000 RAP,DELTAWIN_J2000 DECP, MAG_AUTO_G, TILENAME
FROM Y3_GOLD_2_2
WHERE
RA BETWEEN 40.0 and 41.0 and
DEC BETWEEN -41 and -40 and
ROWNUM < 1001
"""
        query2 = """--
-- Example Query --
-- This query selects stars around the center of glubular cluster M2
SELECT
  COADD_OBJECT_ID,RA,DEC,
  MAG_AUTO_G G,
  MAG_AUTO_R R,
  WAVG_MAG_PSF_G G_PSF,
  WAVG_MAG_PSF_R R_PSF
FROM Y3_GOLD_2_2
WHERE
   RA between 323.36-0.12 and 323.36+0.12 and
   DEC between -0.82-0.12 and -0.82+0.12 and
   WAVG_SPREAD_MODEL_I + 3.0*WAVG_SPREADERR_MODEL_I < 0.005 and
   WAVG_SPREAD_MODEL_I > -1 and
   IMAFLAGS_ISO_G = 0 and
   IMAFLAGS_ISO_R = 0 and
   SEXTRACTOR_FLAGS_G < 4 and
   SEXTRACTOR_FLAGS_R < 4
"""
        queries = []
        queries.append({'desc': 'Sample Basic information', 'query': query0})
        queries.append({'desc': 'Limit Basic information by region and number of rows', 'query': query0ra})
        queries.append({'desc': 'Select stars from M2 Globular Cluster', 'query': query2})
        jjob = []
        jquery = []

        for i in range(len(queries)):
            jjob.append(queries[i]['desc'])
            jquery.append(queries[i]['query'])
        out_dict = [dict(job=jjob[i], jquery=jquery[i]) for i in range(len(jjob))]
        temp = json.dumps(out_dict, indent=4)
        self.write(temp)

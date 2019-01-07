"""
Written by Landon Gelman for use by DES Data Management, 2017-2018.
"""

import tornado.auth
import tornado.escape
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.websocket
from Crypto.Cipher import AES
import base64
import os
import uuid
import Settings
import datetime
import datetime as dt
import MySQLdb as mydb
import yaml
import ea_tasks

def dt_t(entry):
	t = dt.datetime.strptime(entry['time'], '%a %b %d %H:%M:%S %Y')
	return t.strftime('%Y-%m-%d %H:%M:%S')

class BaseHandler(tornado.web.RequestHandler):
	def get_current_user(self):
		return self.get_secure_cookie("usera")

class FileHandler(BaseHandler):
	@tornado.web.asynchronous
	@tornado.web.authenticated
			
	def post(self):
		loc_user = self.get_secure_cookie("usera").decode('ascii').replace('\"', '')
		loc_passw = self.get_secure_cookie("userb").decode('ascii').replace('\"', '')
		user_folder = os.path.join(Settings.WORKDIR, loc_user)+'/'
		cipher = AES.new(Settings.SKEY, AES.MODE_ECB)
		lp = base64.b64encode(cipher.encrypt(loc_passw.rjust(32)))
		db = self.get_secure_cookie("userdb").decode('ascii').replace('\"', '')
		xs = float(self.get_argument("fc_xsize"))
		ys = float(self.get_argument("fc_ysize"))
		gband = self.get_argument("fc_gband") == 'true'
		rband = self.get_argument("fc_rband") == 'true'
		iband = self.get_argument("fc_iband") == 'true'
		zband = self.get_argument("fc_zband") == 'true'
		yband = self.get_argument("fc_yband") == 'true'
		allbands = self.get_argument("fc_all_toggle") == 'true'
		mag = float(self.get_argument("fc_mag"))
		return_cut = self.get_argument("return_cutout_png") == 'true'
		send_email = self.get_argument("fc_send_email") == 'true'
		email = self.get_argument("fc_email")
		name = self.get_argument("fc_name")
		stype = self.get_argument("fc_submit_type")
		
		#if return_cut:
		#	list_only = False
		#else:
		#	list_only = True
		
		if allbands:
			gband = True
			rband = True
			iband = True
			zband = True
			yband = True
		
		print('**************')
		print(xs, ys, 'sizes')
		print(gband, rband, iband, zband, yband, 'bands')
		print(mag, 'magnitude limit')
		print(stype, 'type')
		print(return_cut, 'return_cut')
		print(send_email, 'send_email')
		print(email, 'email')
		print(name, 'name')
		jobid = str(uuid.uuid4()).replace("-","_")	#'57b54f4f-ab85-4e4e-b366-1557c4b3ca0b' #str(uuid.uuid4())
		if xs == 0.0:
			xs = 1.0
		if ys == 0.0:
			ys = 1.0
		if stype == "manual":
			values = self.get_argument("fc_values")
			print(values)
			filename = user_folder+jobid+'.csv'
			F = open(filename, 'w')
			F.write("RA,DEC\n")
			F.write(values)
			F.close()
		if stype == "csvfile":
			fileinfo = self.request.files["csvfile"][0]
			fname = fileinfo['filename']
			extn = os.path.splitext(fname)[1]
			print(fname)
			print(fileinfo['content_type'])
			filename = user_folder+jobid+extn
			with open(filename, 'w') as F:
				F.write(fileinfo['body'].decode('ascii'))
		print('**************')
		folder2 = user_folder+jobid+'/'
		os.system('mkdir -p '+folder2)
		now = datetime.datetime.now()
		input_csv = user_folder + jobid + '.csv'
		run = ea_tasks.make_chart.apply_async(args=[input_csv, loc_user, lp.decode(),
												  folder2, db, xs, ys, jobid, return_cut,
												  send_email, email, 
												  gband, rband, iband, zband, yband, 
												  mag], retry=True, task_id=jobid)
		
		with open('config/desaccess.yaml', 'r') as cfile:
			conf = yaml.load(cfile)['mysql']
		con = mydb.connect(**conf)
		
		tup = tuple([loc_user, jobid, name, 'PENDING', now.strftime('%Y-%m-%d %H:%M:%S'),
					 'finding chart', '', '', '', -1])		
		
		with con:
			cur = con.cursor()
			cur.execute("INSERT INTO Jobs VALUES {0}".format(tup))
		con.close()
		self.set_status(200)
		self.flush()
		self.finish()

"""
Written by Landon Gelman for use by DES Data Management.

Required Files:
	stellar_seds.json
	grri_contours.pkl
	gzzw1_plot.pkl
	spreadmag_plot.pkl
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
		
		boxsize = float(self.get_argument("da_radius"))
		
		addwise = self.get_argument("da_wise") == 'true'
		addvhs = self.get_argument("da_vhs") == 'true'
		
		fluxwav = self.get_argument("da_fluxwav") == 'true'
		magwav = self.get_argument("da_magwav") == 'true'
		grri = self.get_argument("da_grri") == 'true'
		gzzw1 = self.get_argument("da_gzzw1") == 'true'
		spreadmag = self.get_argument("da_spreadmag") == 'true'
		
		send_email = self.get_argument("da_send_email") == 'true'
		email = self.get_argument("da_email")
		name = self.get_argument("da_name")
		
		stype = self.get_argument("da_submit_type")
		
		useCoadds = False
		useCoords = False
		
		print('**************')
		print(boxsize, 'radius')
		print(addwise, addvhs, 'additional surveys')
		print(fluxwav, magwav, grri, gzzw1, spreadmag, 'plots')
		print(stype, 'type')
		print(send_email, 'send_email')
		print(email, 'email')
		print(name, 'name')
		jobid = str(uuid.uuid4()).replace("-","_")
		print(jobid)
		if boxsize == 0.0:
			boxsize = 4.0
		if stype == "manualCoadds":
			useCoadds = True
			values = self.get_argument("da_coadds")
			print(values)
			filename = user_folder+jobid+'.csv'
			F = open(filename, 'w')
			F.write("COADD_OBJECT_ID\n")
			F.write(values)
			F.close()
		if stype == "manualCoords":
			useCoords = True
			values = self.get_argument("da_coords")
			print(values)
			filename = user_folder+jobid+'.csv'
			F = open(filename, 'w')
			F.write("RA,DEC\n")
			F.write(values)
			F.close()
		if stype == "coaddfile":
			useCoadds = True
			fileinfo = self.request.files["csvfile1"][0]
			fname = fileinfo['filename']
			extn = os.path.splitext(fname)[1]
			print(fname)
			print(fileinfo['content_type'])
			filename = user_folder+jobid+extn
			with open(filename, 'w') as F:
				F.write(fileinfo['body'].decode('ascii'))
		if stype == "coordfile":
			useCoords = True
			fileinfo = self.request.files["csvfile2"][0]
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
		input_type = 'coadds' if useCoadds else 'coords'
		
		run = ea_tasks.run_vistools.apply_async(args=[input_type, input_csv, loc_user, 
                                                lp.decode(), folder2, db, boxsize, 
                                                fluxwav, magwav, grri, gzzw1, spreadmag, 
                                                addwise, addvhs, 
                                                jobid, send_email, email], retry=True, task_id=jobid)
		
		with open('config/desaccess.yaml', 'r') as cfile:
			conf = yaml.load(cfile)['mysql']
		con = mydb.connect(**conf)
		
		tup = tuple([loc_user, jobid, name, 'PENDING', now.strftime('%Y-%m-%d %H:%M:%S'),
					 'data analysis', '', '', '', -1])		
		
		with con:
			cur = con.cursor()
			cur.execute("INSERT INTO Jobs VALUES {0}".format(tup))
		con.close()
		self.set_status(200)
		self.flush()
		self.finish()

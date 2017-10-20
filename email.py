import subprocess
import time
import tornado.web
import tornado.websocket
import smtplib
import urllib
import glob
import os, io
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr
import Settings
import requests
import api


def send_test():
    toemail = 'mcarras2@illinois.edu'
    fromemail = 'devnull@ncsa.illinois.edu'
    s = smtplib.SMTP('smtp.ncsa.illinois.edu')
    link = Settings.ROOT_URL
    #link2 = urllib.quote(link.encode('utf8'),safe="%/:=&?~#+!$,;'@()*[]")
    #jobid2=jobid[jobid.find('__')+2:jobid.find('{')-1]
    html = """\
    <html>
    <head></head>
    <body>
         <b> Please do not reply to this email</b> <br><br>
        <p>The job %s was completed, <br>
        the results can be retrieved from this <a href="%s">link</a> under My Jobs Tab.
        </p><br>
        <p> DESDM Thumbs generator</p><br>
        <p> PS: This is the full link to the results: <br>
        %s </p>
    </body>
    </html>
    """ % (jobid, link, link)
    MP1 = MIMEText(html, 'html')
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Job %s is completed' % jobid
    msg['From'] = formataddr((str(Header('DESDM Thumbs', 'utf-8')), fromemail))
    msg['To'] = toemail
    msg.attach(MP1)
    s.sendmail(fromemail, toemail, msg.as_string())
    s.quit()
    return "Email Sent to %s" % toemail



def send_note(user, jobid, toemail):
    print('Task was completed')
    print('I will notify %s to its email address :  %s' % (user, toemail))
    fromemail = 'devnull@ncsa.illinois.edu'
    s = smtplib.SMTP('smtp.ncsa.illinois.edu')
    link = Settings.ROOT_URL
    #link2 = urllib.quote(link.encode('utf8'),safe="%/:=&?~#+!$,;'@()*[]")
    #jobid2=jobid[jobid.find('__')+2:jobid.find('{')-1]



    html = """\
    <html>
    <head></head>
    <body>
         <b> Please do not reply to this email</b> <br><br>
        <p>The job %s was completed, <br>
        the results can be retrieved from this <a href="%s">link</a> under My Jobs Tab.
        </p><br>
        <p> DESDM Thumbs generator</p><br>
        <p> PS: This is the full link to the results: <br>
        %s </p>
    </body>
    </html>
    """ % (jobid, link, link)


    MP1 = MIMEText(html, 'html')

    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Job %s is completed' % jobid
    #msg['From'] = fromemail
    msg['From'] = formataddr((str(Header('DESDM Thumbs', 'utf-8')), fromemail))
    msg['To'] = toemail

    msg.attach(MP1)


    s.sendmail(fromemail, toemail, msg.as_string())
    s.quit()
    return "Email Sent to %s" % toemail


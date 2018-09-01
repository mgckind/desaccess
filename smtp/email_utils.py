import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr
import Settings
import jinja2
import os
import uuid
import base64
import yaml


def render(tpl_path, context):
    path, filename = os.path.split(tpl_path)
    return jinja2.Environment(
        loader=jinja2.FileSystemLoader(path or './')
    ).get_template(filename).render(context)


class SingleAnnounceEmailHeader(object):
    def __init__(self, context, char='m'):
        username = 'allusers'
        self.toemail = 'des-dr-announce@ncsa.illinois.edu'
        self.server = 'smtp.ncsa.illinois.edu'
        # self.server = 'localhost'
        self.fromemail = 'des-dr-announce@ncsa.illinois.edu'
        self.s = smtplib.SMTP(self.server)
        self.msg = MIMEMultipart('alternative')
        self.msg['Subject'] = '[Announcement] '+context['Subject']
        self.msg['From'] = formataddr((str(Header('DESDM Release Team', 'utf-8')), self.fromemail))
        self.msg['To'] = self.toemail
        self.uid = str(uuid.uuid4())+'-{0}'.format(char)+base64.b64encode(username.encode()).decode('ascii')
        self.context = context
        self.context['email_link'] += self.uid
        self.html = render('easyweb/static/internal/templates/template_announce.html', self.context)
        with open('easyweb/static/internal/emails/'+self.uid+'.html', 'w') as ff:
            ff.write(self.html)

class SingleEmailHeader(object):
    def __init__(self, username, toemail, context, char='r', ps=None):
        self.toemail = toemail
        self.server = 'smtp.ncsa.illinois.edu'
        # self.server = 'localhost'
        self.fromemail = 'devnull@ncsa.illinois.edu'
        self.s = smtplib.SMTP(self.server)
        self.msg = MIMEMultipart('alternative')
        self.msg['Subject'] = context['Subject']
        self.msg['From'] = formataddr((str(Header('DESDM Release Team', 'utf-8')), self.fromemail))
        self.msg['To'] = self.toemail
        self.uid = str(uuid.uuid4())+'-{0}'.format(char)+base64.b64encode(username.encode()).decode('ascii')
        self.context = context
        self.context['email_link'] += self.uid
        if ps is None:
            self.ps = 'PS: This is the full link you can copy/paste into the browser:<br /> <span style="font-size: 11px">{link}</span>'.format(link=self.context['link'])
        else:
            self.ps = ps
        self.context['ps'] = self.ps
        self.html = render('easyweb/static/internal/templates/template.html', self.context)
        with open('easyweb/static/internal/emails/'+self.uid+'.html', 'w') as ff:
            ff.write(self.html)


def send_activation(firstname, username, email, url):
    bcc = 'mgckind@gmail.com'
    link = Settings.ROOT_URL + '/easyweb/activate/{0}'.format(url)
    context = {
        "Subject": "DESaccess DB Activation link",
        "email_link": Settings.ROOT_URL+'/easyweb/email/',
        "username": firstname,
        "msg": """Welcome!<br>
        You need to activate your account
        before accessing DESaccess services. <br > The activation link is valid
        for the next 12 hours""",
        "action": "Click Here To Activate Your Account",
        "link": link,
    }
    header = SingleEmailHeader(username, email, context, char='a')
    MP1 = MIMEText(header.html, 'html')
    header.msg.attach(MP1)
    header.s.sendmail(header.fromemail, [header.toemail, bcc], header.msg.as_string())
    header.s.quit()
    return "Email Sent to %s" % header.toemail


def send_reset(email, username, url):
    bcc = 'mgckind@gmail.com'
    link = Settings.ROOT_URL + '/easyweb/reset/{0}'.format(url)
    context = {
        "Subject": "DESaccess DB Reset link",
        "email_link": Settings.ROOT_URL+'/easyweb/email/',
        "username": username,
        "msg": """You have have requested to reset your password <br > The reset link is valid
        for the next 24 hours""",
        "action": "Click Here To Reset Your Password",
        "link": link,
    }
    header = SingleEmailHeader(username, email, context, char='r')
    MP1 = MIMEText(header.html, 'html')
    header.msg.attach(MP1)
    header.s.sendmail(header.fromemail, [header.toemail, bcc], header.msg.as_string())
    header.s.quit()
    return "Email Sent to %s" % header.toemail


def send_test(username):
    bcc = 'mgckind@gmail.com'
    link = 'http//localhost'
    context = {
        "Subject": "This is a test",
        "email_link": Settings.ROOT_URL+'/easyweb/email/',
        "username": username,
        "msg": """You have have requested to reset your password <br > The reset link is valid
        for the next 24 hours""",
        "action": "Click Here To Reset Your Password",
        "link": link,
    }
    header = SingleEmailHeader(username, 'mgckind@gmail.com', context, char='t')
    MP1 = MIMEText(header.html, 'html')
    header.msg.attach(MP1)
    header.s.sendmail(header.fromemail, [header.toemail, bcc], header.msg.as_string())
    header.s.quit()
    return "Email Sent to %s" % header.toemail


def send_note(username, jobid, toemail):
    bcc = 'mgckind@gmail.com'
    link = Settings.ROOT_URL+'/easyweb/my-jobs'
    context = {
        "Subject": "Job {} is completed".format(jobid),
        "email_link": Settings.ROOT_URL+'/easyweb/email/',
        "username": username,
        "msg": """The job <b>{}</b> was completed. <br>
        The results can be retrieved from the link below""".format(jobid),
        "action": "Click Here To See Your Jobs",
        "link": link,
    }
    header = SingleEmailHeader(username, toemail, context, char='c')
    MP1 = MIMEText(header.html, 'html')
    header.msg.attach(MP1)
    header.s.sendmail(header.fromemail, [header.toemail, bcc], header.msg.as_string())
    header.s.quit()
    return "Email Sent to %s" % header.toemail

def send_fail(username, jobid, toemail):
    bcc = 'mgckind@gmail.com'
    link = Settings.ROOT_URL+'/easyweb/my-jobs'
    context = {
        "Subject": "Job {} failed".format(jobid),
        "email_link": Settings.ROOT_URL+'/easyweb/email/',
        "username": username,
        "msg": """The job <b>{}</b> was canceled or failed. <br>
        The log can be retrieved from the link below""".format(jobid),
        "action": "Click Here To See Your Jobs/Logs",
        "link": link,
    }
    header = SingleEmailHeader(username, toemail, context, char='c')
    MP1 = MIMEText(header.html, 'html')
    header.msg.attach(MP1)
    header.s.sendmail(header.fromemail, [header.toemail, bcc], header.msg.as_string())
    header.s.quit()
    return "Email Sent to %s" % header.toemail


def send_thanks(name, email, subject, ticket):
    bcc = 'mgckind@gmail.com'
    context = {
        "Subject": "[{0}] : {1} ".format(ticket, subject),
        "email_link": Settings.ROOT_URL+'/easyweb/email/',
        "username": name,
        "msg": """We have received your form and a ticket with name <b>{ticket}</b> <br>
         was created. We will get in touch with you soon. Please use the ticket <br>
         name and number  for  future communications.""".format(ticket=ticket),
        "action": "No extra action is required",
        "link": "#",
    }
    header = SingleEmailHeader(name, email, context, char='j', ps='')
    MP1 = MIMEText(header.html, 'html')
    header.msg.attach(MP1)
    header.s.sendmail(header.fromemail, [header.toemail, bcc], header.msg.as_string())
    header.s.quit()
    return "Email Sent to %s" % header.toemail


def subscribe_email(email):
    with open('config/desaccess.yaml', 'r') as cfile:
        conf = yaml.load(cfile)['majordomo']
    server = 'smtp.ncsa.illinois.edu'
    #fromemail = 'devnull@ncsa.illinois.edu'
    fromemail = 'mcarras2@illinois.edu'
    s = smtplib.SMTP(server)
    msg = MIMEMultipart('alternative')
    msg['Subject'] = ''
    msg['From'] = fromemail
    msg['To'] = 'majordomo@ncsa.illinois.edu'
    body = "approve {passwd} subscribe des-dr-announce {email}".format(passwd=conf['passwd'],
                                                                       email=email)
    MP1 = MIMEText(body)
    msg.attach(MP1)
    s.sendmail(fromemail, 'majordomo@ncsa.illinois.edu', msg.as_string())
    s.quit()

def unsubscribe_email(email):
    with open('config/desaccess.yaml', 'r') as cfile:
        conf = yaml.load(cfile)['majordomo']
    server = 'smtp.ncsa.illinois.edu'
    #fromemail = 'devnull@ncsa.illinois.edu'
    fromemail = 'mcarras2@illinois.edu'
    s = smtplib.SMTP(server)
    msg = MIMEMultipart('alternative')
    msg['Subject'] = ''
    msg['From'] = fromemail
    msg['To'] = 'majordomo@ncsa.illinois.edu'
    body = "approve {passwd} unsubscribe des-dr-announce {email}".format(passwd=conf['passwd'],
                                                                       email=email)
    MP1 = MIMEText(body)
    msg.attach(MP1)
    s.sendmail(fromemail, 'majordomo@ncsa.illinois.edu', msg.as_string())
    s.quit()


def send_announce(subject, msg):
    with open('config/desaccess.yaml', 'r') as cfile:
        conf = yaml.load(cfile)['majordomo']
    context = {
        "Subject": "{0}".format(subject),
        "email_link": Settings.ROOT_URL+'/easyweb/email/',
        "msg": msg,
        "link": "#",
    }
    header = SingleAnnounceEmailHeader(context)
    MP1 = MIMEText(header.html, 'html')
    header.msg.attach(MP1)
    msg2 = 'Approved: {0}\n'.format(conf['passwd']) + header.msg.as_string()
    header.s.sendmail(header.fromemail, header.toemail, msg2)
    header.s.quit()

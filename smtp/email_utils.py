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


def render(tpl_path, context):
    path, filename = os.path.split(tpl_path)
    return jinja2.Environment(
        loader=jinja2.FileSystemLoader(path or './')
    ).get_template(filename).render(context)


class SingleEmailHeader(object):
    def __init__(self, username, toemail, context, char='r'):
        self.toemail = toemail
        self.server = 'smtp.ncsa.illinois.edu'
        #self.server = ' localhost'
        self.fromemail = 'devnull@ncsa.illinois.edu'
        self.s = smtplib.SMTP(self.server)
        self.msg = MIMEMultipart('alternative')
        self.msg['Subject'] = context['Subject']
        self.msg['From'] = formataddr((str(Header('DESDM Release Team', 'utf-8')), self.fromemail))
        self.msg['To'] = self.toemail
        self.uid = str(uuid.uuid4())+'-{0}'.format(char)+base64.b64encode(username.encode()).decode('ascii')
        self.context = context
        self.context['email_link'] += self.uid
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
        for the next 12 hours""",
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
    link = 'http//localhost:8080'
    context = {
        "Subject": "This is a test",
        "email_link": Settings.ROOT_URL+'/easyweb/email/',
        "username": username,
        "msg": """You have have requested to reset your password <br > The reset link is valid
        for the next 12 hours""",
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

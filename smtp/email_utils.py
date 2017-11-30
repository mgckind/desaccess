import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr
import Settings


def send_activation(firstname, username, email, url):
    toemail = email
    bcc = 'mgckind@gmail.com'
    fromemail = 'devnull@ncsa.illinois.edu'
    s = smtplib.SMTP('smtp.ncsa.illinois.edu')
    link = Settings.ROOT_URL + '/easyweb/activate/{0}'.format(url)
    html = """\
    <html>
    <head></head>
    <body>
        <b> Please do not reply to this automatic email</b> <br><br>
        <p> Dear {firstname}, <p> <br>
        <p> Click on this <a href="{link}">link</a> to activate your account</p>
        <br>
        <p> DESDM Release Team</p><br>
        <p> PS: This is the full activation link : <br>
        {link} </p>
    </body>
    </html>
    """.format(firstname=firstname, link=link)
    MP1 = MIMEText(html, 'html')
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'DESaccess  Activation link'
    msg['From'] = formataddr((str(Header('DESDM Release Team', 'utf-8')), fromemail))
    msg['To'] = toemail
    msg.attach(MP1)
    s.sendmail(fromemail, [toemail, bcc], msg.as_string())
    s.quit()


def send_reset(email, url):
    toemail = email
    bcc = 'mgckind@gmail.com'
    fromemail = 'devnull@ncsa.illinois.edu'
    s = smtplib.SMTP('smtp.ncsa.illinois.edu')
    link = Settings.ROOT_URL + '/easyweb/reset/{0}'.format(url)
    html = """\
    <html>
    <head></head>
    <body>
        <b> Please do not reply to this automatic email</b> <br><br>
        <p> Dear User, <p> <br>
        <p> Click on this <a href="{link}">link</a> to reset your password</p>
        <p> Note that the link will automatically expire soon </p>
        <br>
        <p> DESDM Release Team</p><br>
        <p> PS: This is the full reset link : <br>
        {link} </p>
    </body>
    </html>
    """.format(link=link)
    MP1 = MIMEText(html, 'html')
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'DESaccess DB Reset link'
    msg['From'] = formataddr((str(Header('DESDM Release Team', 'utf-8')), fromemail))
    msg['To'] = toemail
    msg.attach(MP1)
    s.sendmail(fromemail, [toemail, bcc], msg.as_string())
    s.quit()

def send_test():
    toemail = 'mcarras2@illinois.edu'
    fromemail = 'devnull@ncsa.illinois.edu'
    s = smtplib.SMTP('smtp.ncsa.illinois.edu')
    link = 'localhost'#Settings.ROOT_URL
    jobid = 'test'
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
        <p> Dear User, <p> 
        <p>The job %s was completed, <br>
        the results can be retrieved from this <a href="%s">link</a> under My Jobs Tab.
        </p><br>
        <p> DESDM Release Team</p><br>
        <p> PS: This is the full link to the results: <br>
        %s </p>
    </body>
    </html>
    """ % (jobid, link, link)

    MP1 = MIMEText(html, 'html')
    msg = MIMEMultipart('alternative')
    msg['Subject'] = 'Job %s is completed' % jobid
    # msg['From'] = fromemail
    msg['From'] = formataddr((str(Header('DESDM Release Team', 'utf-8')), fromemail))
    msg['To'] = toemail
    msg.attach(MP1)
    s.sendmail(fromemail, toemail, msg.as_string())
    s.quit()
    return "Email Sent to %s" % toemail

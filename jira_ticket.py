"""
This utility cretae jira tickets from help form
"""
from jira.client import JIRA
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header
from email.utils import formataddr
import smtplib
import urllib

def send_email():
    subject = "New Ticket in DESRELEASE"
    toemail = 'mcarras2@illinois.edu'
    fromemail = 'devnull@ncsa.illinois.edu'
    s = smtplib.SMTP('smtp.ncsa.illinois.edu')
    text = "https://opensource.ncsa.illinois.edu/jira/projects/DESRELEASE"
    MP1 = MIMEText(text, 'plain')
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = formataddr((str(Header('DESRELEASE JIRA', 'utf-8')), fromemail))
    msg['To'] = toemail
    msg.attach(MP1)
    s.sendmail(fromemail, toemail, msg.as_string())
    s.quit()


def create_ticket(first, last, email, topics, subject, question):
    f = open('.access', 'r')
    A = f.readlines()
    f.close()
    my_string_u = base64.b64decode(A[0].strip()).decode().strip()
    my_string_p = base64.b64decode(A[1].strip()).decode().strip()
    """
    This function creates the ticket coming form the help form
    """

    jira = JIRA(
        server="https://opensource.ncsa.illinois.edu/jira/",
        basic_auth=(my_string_u, my_string_p))

    body = """
    *ACTION ITEMS*
    - Please ASSIGN this ticket if it is unassigned.
    - PLEASE SEND AN EMAIL TO  *%s* to reply to this ticket
    - COPY the answer in the comments section and ideally further communication.
    - PLEASE close this ticket when resolved


    *Name*: %s %s

    *Email*: %s

    *Topics*:
    %s

    *Question*:
    %s

    """ % (email, first, last, email, topics, question)

    issue = {
        'project' : {'key': 'DESRELEASE'},
        'issuetype': {'name': 'Task'},
        'summary': 'Q: %s' % subject,
        'description' : body,
        #'reporter' : {'name': 'desdm-wufoo'},
        }
    jira.create_issue(fields=issue)
    #send_email()

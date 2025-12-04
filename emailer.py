# emailer.py
import smtplib
from email.mime.text import MIMEText
from config import SMTP, EMAIL
import logging

log = logging.getLogger(__name__)

def send_email(to_addrs, subject, body):
    msg = MIMEText(body, "plain")
    msg['Subject'] = f"{EMAIL['subject_prefix']} {subject}"
    msg['From'] = SMTP['from_addr']
    msg['To'] = ", ".join(to_addrs if isinstance(to_addrs, (list,tuple)) else [to_addrs])

    try:
        server = smtplib.SMTP(SMTP['host'], SMTP['port'], timeout=20)
        if SMTP.get('use_tls'):
            server.starttls()
        if SMTP.get('user'):
            server.login(SMTP['user'], SMTP['password'])
        server.sendmail(SMTP['from_addr'], to_addrs if isinstance(to_addrs, list) else [to_addrs], msg.as_string())
        server.quit()
        log.info("Email sent to %s", to_addrs)
        return True
    except Exception as e:
        log.exception("Failed to send email: %s", e)
        return False

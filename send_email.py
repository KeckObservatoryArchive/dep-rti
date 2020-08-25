def send_email(toEmail, fromEmail, subject, message):
    '''
    Sends email using the input parameters

    @type toEmail: str
    @param toEmail: email recipient
    @type fromEmail: str
    @param fromEmail: email sender
    @type subject: str
    @param subject: subject of message
    @type message: str
    @param message: body of email
    '''

    import smtplib
    from email.mime.text import MIMEText

    # Construct email message

    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['To'] = toEmail
    msg['From'] = fromEmail

    # Send the email

    s = smtplib.SMTP('localhost')
    s.send_message(msg)
    s.quit()
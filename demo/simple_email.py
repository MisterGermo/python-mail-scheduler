__filename = 'simple_email.py'
__fname = 'simple_email'
cStrDivider = '#================================================================#'
print('', cStrDivider, f'START _ {__filename}', cStrDivider, sep='\n')
print(f'GO {__filename} -> starting IMPORTs and globals decleration')

import smtplib
#============================================================================#
# SMTP email
SES_SERVER = 'mail.xxx.com'
SES_PORT = 465
SES_LOGIN = "xxx@xxxx.com"
SES_PASSWORD = "xxx"
SENDER_EMAIL = SES_LOGIN
LST_RECEIVERS = ['xxx@xxx.com', 'yyyy@yyy.com']

#============================================================================#
#============================================================================#

def sendEmailTest(test_id=-1, dev_msg='nil'):
    funcname = f'({__filename}) sendEmailTest'
    print(funcname)
    fparams = f'fparams:\n   test_id={test_id}\n   dev_msg={dev_msg}'
    print(fparams)

    receivers = [SES_RECEIVER]
    sender = SES_ADMIN
    subject = f"server test email [{test_id}]"
    body = f"Server test email body...\n\n\n     dev_msg: {dev_msg}\n\n\n _END_\n\n"
    return sendTextEmail(sender, receivers, subject, body)

def email_send_weekly(subj='nil_subj', msg='nil_msg', lst_receivers=[]):
    funcname = f'({__filename}) email_send_weekly'
    fparams = f'fparams:\n   msg={msg}\n    lst_receivers={lst_receivers}'
    sender = SENDER_EMAIL
    return sendTextEmail(sender, lst_receivers, subj, msg)

def sendTextEmail(sender_email, lst_recipients, subject, text):
    funcname = f'({__filename}) sendTextEmail'
    fparams = f'fparams:\n   sender_email={sender_email}\n   lst_recipients={lst_recipients}\n   subject={subject}\n   text={text}'

    #funparams = f"From: {sender_email}\r\nTo: %s\r\nSubject: {subject}\r\nBody: \n{text}" % ",".join(lst_recipients)
    lstRec = ",".join(lst_recipients)
    funparams = f"From: {sender_email}\r\nTo: {lstRec}\r\nSubject: {subject}\r\nBody: \n{text}"
    print(f"START -> sendTextEmail w/ subject: '{subject}'")
    iDebugLvl = 3
    try:
        # note (RFC 5322): this syntax must remain (i.e. cannot pre-pend new line)
        msg = f"From: {sender_email}\r\nTo: %s\r\nSubject: {subject}\r\n\r\n" % ",".join(lst_recipients)
        server = smtplib.SMTP_SSL(SES_SERVER, SES_PORT)
        #server.set_debuglevel(iDebugLvl)
        server.ehlo()
        server.login(SES_LOGIN, SES_PASSWORD)
        #print(f'\nHeader... \n{msg}', f'\nText... \n{text}')
        strMsgEncode = getStrEncodeUTF8(msg + text)
        server.sendmail(sender_email, lst_recipients, strMsgEncode)
        server.quit()
        print(funcname, 'END -> sendTextEmail SUCCESS', '\n')
        return True, 'no exception'

    except Exception as e: # ref: https://docs.python.org/2/tutorial/errors.html
        print(f"  Exception caught during Send Email attempt", f"\n  ...ATTEMPTING to RE-SEND EMAIL with 'server.set_debuglevel({iDebugLvl})' enabled\n")
        try:
            server = smtplib.SMTP_SSL(SES_SERVER, SES_PORT)
            server.set_debuglevel(iDebugLvl)
            server.ehlo()
            server.login(SES_LOGIN, SES_PASSWORD)
            server.sendmail(sender_email, lst_recipients, msg + text)
            server.quit()
            print('     re-send email succeeded this time! wtf?', f'FuncParamsPassed... \n{funparams}\n')
            return True, f'no exception on retry; first e: {e}'
        except UnicodeEncodeError as e:
            print(f"  UnicodeEncodeError Exception caught during RE-SEND EMAIL attempt w/ 'server.set_debuglevel({iDebugLvl})' enabled\n", "\n  returning False and continuing callstack")
            return False, f'UnicodeEncodeError exception: {e}'
        except Exception as e:
            strFunParamsRepr = f"\nFuncParamsPassed... repr(funparams)... \n\n{repr(funparams)}\n\n"
            strFuncParams = strFunParamsRepr + f"\nFuncParamsPassed... \n\n{funparams}\n\n"
            print(f"  Exception caught during RE-SEND EMAIL attempt w/ 'server.set_debuglevel({iDebugLvl})' enabled... {e}\n  returning False and continuing callstack", strFuncParams)
            return False, f'exception: {e}'

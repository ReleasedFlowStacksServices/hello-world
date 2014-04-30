import sys, uuid, json
from datetime                       import datetime, date, timedelta

sys.path.append("/flowstacks/public-cloud-src")
from logger.logger import Logger
from modules.base_api.fs_web_tier_base_work_item     import FSWebTierBaseWorkItem
from connectors.redis.redis_pickle_application       import RedisPickleApplication

class RA_SendEmailTemplateToUsers(FSWebTierBaseWorkItem):

    def __init__(self, json_data):
        FSWebTierBaseWorkItem.__init__(self, "RA_SETTU", json_data)

        """ Constructor Serialization taking HTTP Post-ed JSON into Python members """
        # Define Inputs and Outputs for the Job to serialize over HTTP
        try:

            # INPUTS:
            self.m_user_name                        = str(json_data["User Name"])
            self.m_user_email                       = str(json_data["User Email Address"])
            self.m_from                             = str(json_data["From"])
            self.m_subject                          = str(json_data["Subject"])
            self.m_email_template                   = str(json_data["Email Template"])
            self.m_gmail_user                       = str(json_data["Gmail User"])
            self.m_gmail_password                   = str(json_data["Gmail Password"])
        
            # OUTPUTS:
            self.m_results["Status"]                = "FAILED"
            self.m_results["Error"]                 = ""
        
            # MEMBERS:
            self.m_debug                            = False

        # Return the exact Error with the failure:
        except Exception,e:

            import os, traceback
            exc_type, exc_obj, exc_tb = sys.exc_info()
            reason = json.dumps({ "Module" : str(self.__class__.__name__), "Error Type" : str(exc_type.__name__), "Line Number" : exc_tb.tb_lineno, "Error Message" : str(exc_obj.message), "File Name" : str(os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]) })
            raise Exception(reason)

    # end of  __init__


###############################################################################
#
# Request Handle Methods
#
###############################################################################


    def handle_startup(self):

        self.lg("Start Handle Startup", 5)

        self.m_state = "Results"
        self.m_results["Status"] = "SUCCESS"
        self.handle_sending_email_template_to_user()

        self.lg("Done Startup State(" + self.m_state + ")", 5)

        return None
    # end of handle_startup

    
    def handle_sending_email_template_to_user(self):

        self.lg("Sending email to user", 5)

        try:

            import smtplib
            from email.mime.text import MIMEText
            
            # Connect to the SMTP Server google is hosting for the User
            email_server    = smtplib.SMTP("smtp.gmail.com", 587)
            user            = self.m_gmail_user
            pw              = self.m_gmail_password
            self.lg("Booting up ehlo 1", 5)
            email_server.ehlo()
            self.lg("Starting TLS", 5)
            email_server.starttls()
            self.lg("Starting ehlo 2", 5)
            email_server.ehlo()
            self.lg("Logging in", 5)
            email_server.login(user, pw) 

            self.lg("Sending Email with Notification Subject(" +str(self.m_subject) + ")", 5)

            # 2) Perform regex substituion on <USER.NAME> with current user name
            template = self.perform_template_substitution()
            # 3) Try
            try:
                # 3a) Create message
                msg = MIMEText(template, 'html')
                msg['Subject'] = self.m_subject
                msg['From'] = self.m_from
                msg['To'] = self.m_user_email
                # 3b) Send email
                self.lg("Actually sending email to " + self.m_user_email)
                email_server.sendmail(self.m_from, self.m_user_email, msg.as_string())
                # 3c) Set status to success
                self.m_results["Status"] = "SUCCESS"
            except Exception, f:
                self.lg("ERROR: Sending Email To User " + self.m_user_email, 5)
                self.lg("ERROR: Encountered an exception: %s" % (f,), 5)
                # 4) Catch error - set status to fail with reason
                self.m_results["Status"]    = "ERROR"
                self.m_results["Exception"] = str(f)
                
                return None
                         
            email_server.quit()
        except Exception, e:
            self.lg("ERROR: Sending Email", 0)
            self.lg("ERROR: Encountered an exception: %s" % (e,), 0)
            self.m_results["Status"] = "ERROR"
            self.m_results["Exception"] = str(e)

        return None
    # end of handle_sending_email

            
    def handle_processing_results(self):

        self.lg("Processing Results", 5)

        self.lg("Done Processing Results", 5)

        return None
    # end of handle_processing_results


###############################################################################
#
# Helpers
#
###############################################################################


    def perform_template_substitution(self):
        template = self.m_email_template
        template.replace("#USER.NAME", self.m_user_name)
        
        self.lg("Template(" + str(template) + ")", 5)
        return template
    # end of perform_template_substitution
    

###############################################################################
#
# Request State Machine
#
###############################################################################


    # Needs to be state driven:
    def perform_task(self):

        if  self.m_state == "Startup":
            self.lg("Startup", 5)
            self.handle_startup()

        elif self.m_state == "Results":
            # found in the base
            self.lg("Result Cleanup", 5)
            self.handle_processing_results()
            self.base_handle_results_and_cleanup(self.m_result_details, self.m_completion_details)

        else:
            if self.m_log:
                self.lg("UNKNOWN STATE FOUND IN OBJECT(" + self.m_name + ") State(" + self.m_state + ")", 0)
            self.m_state = "Results"

        # end of State Loop
        return self.m_is_done
    # end of perform_task

# end of RA_SendEmailTemplateToUsers




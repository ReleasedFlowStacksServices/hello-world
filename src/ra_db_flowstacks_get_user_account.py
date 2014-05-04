import sys, uuid, json
from datetime                       import datetime, date, timedelta

sys.path.append("/flowstacks/public-cloud-src")
from logger.logger import Logger
from modules.base_api.fs_web_tier_base_work_item    import FSWebTierBaseWorkItem
from connectors.redis.redis_pickle_application      import RedisPickleApplication

# Endpoint Database Schema Files:
from dev_db_schema                                  import LT_UserStatus, PT_UserAccount
from prod_db_schema                                 import LT_UserStatus, PT_UserAccount

class RA_DBF_GetUserAccount(FSWebTierBaseWorkItem):

    def __init__(self, json_data):
        FSWebTierBaseWorkItem.__init__(self, "RA_DBF_GUA", json_data)

        """ Constructor Serialization taking HTTP Post-ed JSON into Python members """
        # Define Inputs and Outputs for the Job to serialize over HTTP
        try:

            # INPUTS:
            self.m_db_app_name                  = str(json_data["DB To Use"])
            self.m_user_name                    = str(json_data["User Name"])
            self.m_query_type                   = str(json_data["Query Type"])

            # OUTPUTS:
            self.m_results["Status"]            = "FAILED"
            self.m_results["Error"]             = ""
            self.m_results["Record"]            = {}

            # MEMBERS:
            self.m_db_record                    = None

            if "Debug" in json_data:
                self.m_debug                    = bool(str(json_data["Debug"]) == "True")

        except Exception,e:
            import os, traceback
            exc_type, exc_obj, exc_tb = sys.exc_info()
            reason = json.dumps({ "Module" : str(self.__class__.__name__), "Error Type" : str(exc_type.__name__), "Line Number" : exc_tb.tb_lineno, "Error Message" : str(exc_obj.message), "File Name" : str(os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]) })
            raise Exception(reason)
        # end of try/catch

    # end of  __init__


###############################################################################
#
# Job Module Handle Each State Methods
#
###############################################################################


    def handle_startup(self):

        self.lg("Start Handle Module Startup", 5)

        error_msg                   = ""
        self.m_state                = "Results"
        
        try:
            
            self.lg("Connect and Commit User DB(" + str(self.m_db_app_name) + ")", 5)

            self.lg("Connect and to DB(" + str(self.m_db_app_name) + ")", 5)

            self.connect(self.m_db_app_name)

            if   self.m_query_type  == "Get User By Name":
                self.handle_get_user_by_user_name()

            else:
                self.lg("Unsupported Query Type(" + str(self.m_query_type) + ")", 0)
                self.m_results["Status"]    = "FAILED"
                self.m_results["Error"]     = "Unsupported Query Type " + str(self.m_query_type)

        except Exception,e:

            self.lg("Getting User Account Failed Error(" + str(error_msg) + ") Exception(" + str(e) + ")", 0)
            self.m_results["Status"]    = "FAILED"
            self.m_results["Error"]     = error_msg

        # end of adding user record to db

        self.lg("Done Module Startup State(" + self.m_state + ")", 5)

        return None
    # end of handle_startup


    def handle_processing_results(self):

        self.lg("Processing Results", 5)

        self.convert_db_records_into_results()

        self.lg("Done Processing Results", 5)

        return None
    # end of handle_processing_results


###############################################################################
#
# Helpers
#
###############################################################################


    def handle_get_user_by_user_name(self):

        self.lg("Getting User By Name", 5)

        self.m_db_record    = self.m_session.query(PT_UserStatus).filter(PT_UserStatus.user_name == self.m_user_name).first()

        self.lg("End Getting User By Name(" + str(self.m_db_record) + ")", 5)

        return None
    # end of handle_get_user_by_user_name


    def convert_db_records_into_results(self):

        self.lg("Convert DB Record", 5)

        try:

            if(self.m_db_record):

                db_hash = {}
                db_hash = {
                            "ID"            : str(self.m_db_record.id),
                            "First Name"    : str(self.m_db_record.status),
                            "Last Name"     : str(self.m_db_record.status),
                            "User Name"     : str(self.m_db_record.status),
                            "Email"         : str(self.m_db_record.status),
                            "Status"        : str(self.m_db_record.user_status_sym.status)
                }

                self.m_results["Status"]    = "SUCCESS"
                self.m_results["Error"]     = ""
                self.m_results["Record"]    = db_hash

            else:

                self.m_results["Status"]    = "FAILED"
                self.m_results["Error"]     = "Did Not Find User"

            # end of if there are db records

        except Exception,e:

            self.lg("Convert Exception(" + str(e) + ")", 0)
            self.m_results["Status"]    = "FAILED"
            self.m_results["Error"]     = "Exception Getting User"

        # end of Convert DB Record(s)  

        self.lg("End Convert DB Record", 5)

        return None
    # end of convert_db_records_into_results


###############################################################################
#
# Job Module State Machine
#
###############################################################################


    # Add and Extend New States as Needed:
    def perform_task(self):

        if  self.m_state == "Startup":
            self.lg("Startup", 5)
            self.handle_startup()
            self.handle_processing_results()
            self.base_handle_results_and_cleanup(self.m_result_details, self.m_completion_details)

        else:
            if self.m_log:
                self.lg("UNKNOWN STATE FOUND IN OBJECT(" + self.m_name + ") State(" + self.m_state + ")", 0)
            self.m_state = "Results"

        # end of State Loop
        return self.m_is_done
    # end of perform_task

# end of RA_DBF_GetUserAccount



import sys, uuid, json
from datetime                       import datetime, date, timedelta

sys.path.append("/flowstacks/public-cloud-src")
from logger.logger import Logger
from modules.base_api.fs_web_tier_base_work_item    import FSWebTierBaseWorkItem
from connectors.redis.redis_pickle_application      import RedisPickleApplication

# Endpoint Database Schema Files:
from dev_db_schema                                  import LT_UserStatus, PT_UserAccount
from prod_db_schema                                 import LT_UserStatus, PT_UserAccount

class RA_DBF_CreateUserStatus(FSWebTierBaseWorkItem):

    def __init__(self, json_data):
        FSWebTierBaseWorkItem.__init__(self, "RA_DBF_CUS", json_data)

        """ Constructor Serialization taking HTTP Post-ed JSON into Python members """
        # Define Inputs and Outputs for the Job to serialize over HTTP
        try:

            # INPUTS:
            self.m_db_app_name                  = str(json_data["DB To Use"])
            self.m_new_status                   = str(json_data["New User Status"])

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

            self.create_database_record() 

            db_result_hash = self.just_add_record_to_database(self.m_db_app_name, self.m_db_record)

            if db_result_hash["Status"] == "SUCCESS":

                db_hash = {
                            "ID"        : str(self.m_db_record.id),
                            "Status"    : str(self.m_db_record.status),
                }

                self.m_results["Status"]    = "SUCCESS"
                self.m_results["Error"]     = ""
                self.m_results["Record"]    = db_hash

            else:
                self.lg("ERROR: Failed to Add Record(" + str(self.m_db_record) + ") to DB", 0)
                self.m_results["Status"]    = "Failed to Add Database Record"
                self.m_results["Error"]     = db_result_hash["Error"]

        except Exception,e:

            self.lg("Adding User Status Failed Error(" + str(error_msg) + ") Exception(" + str(e) + ")", 0)
            self.m_results["Status"]    = "FAILED"
            self.m_results["Error"]     = error_msg

        # end of adding user record to db

        self.lg("Done Module Startup State(" + self.m_state + ")", 5)

        return None
    # end of handle_startup


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


    def create_database_record(self):
    
        self.m_db_record    = LT_UserStatus(status = self.m_new_status)

        return None
    # end of create_database_record


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

# end of RA_DBF_CreateUserStatus



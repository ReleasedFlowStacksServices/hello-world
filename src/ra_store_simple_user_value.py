import sys, uuid, json
from datetime                       import datetime, date, timedelta

sys.path.append("/flowstacks/public-cloud-src")
from logger.logger import Logger
from modules.base_api.fs_web_tier_base_work_item     import FSWebTierBaseWorkItem
from connectors.redis.redis_pickle_application       import RedisPickleApplication

class RA_StoreSimpleUserValue(FSWebTierBaseWorkItem):

    def __init__(self, json_data):
        FSWebTierBaseWorkItem.__init__(self, "RA_SSUV", json_data)

        """ Constructor Serialization taking HTTP Post-ed JSON into Python members """
        # Define Inputs and Outputs for the Job to serialize over HTTP
        try:

            # INPUTS:
            self.m_access_token                     = str(json_data["Access Token"])
            self.m_value_to_store                   = str(json_data["Value To Store"])

            # OUTPUTS:
            self.m_results["Status"]                = "FAILED"
            self.m_results["Error"]                 = ""
            self.m_results["Storage Key ID"]        = ""

            # MEMBERS:
            self.m_debug                            = False
            self.m_storage_key_id                   = ""
            self.m_storage_type                     = "SIMPLE"
            self.m_hash_to_store                    = {}

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

        self.m_results["Status"]    = "SUCCESS"
        self.handle_storing_in_cache()

        self.lg("Done Startup State(ENDING)", 5)

        return None
    # end of handle_startup

        
    def handle_storing_in_cache(self):

        ra_name = "BLOB"
        self.m_storage_key_id                   = "StorageID_" + str(self.m_job_id).replace(' ', '')
        self.lg("Storing in Key(" + str(self.m_storage_key_id) + ") RA(" + str(ra_name) + ")", 5)
        self.m_results["Storage Key ID"]        = self.m_storage_key_id

        self.m_hash_to_store = {
                                "Access Token"          : self.m_access_token,
                                "Job ID"                : self.m_job_id,
                                "Value To Store"        : self.m_value_to_store
        }
        self.put_into_app_key(ra_name, self.m_storage_key_id, self.m_hash_to_store)

        self.lg("Done Storing in Key", 5)

        return None
    # end of handle_storing_in_cache
    
            
###############################################################################
#
# Helpers
#
###############################################################################


###############################################################################
#
# Request State Machine
#
###############################################################################


    # Needs to be state driven:
    def perform_task(self):

        if  self.m_state == "Startup":

            self.handle_startup()

            # found in the base
            self.lg("Result Cleanup", 5)
            self.base_handle_results_and_cleanup(self.m_result_details, self.m_completion_details)

        else:
            if self.m_log:
                self.lg("UNKNOWN STATE FOUND IN OBJECT(" + self.m_name + ") State(" + self.m_state + ")", 0)
            self.m_state = "Startup"

        # end of State Loop
        return self.m_is_done
    # end of perform_task

# end of RA_StoreSimpleUserValue




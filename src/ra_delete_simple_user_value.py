import sys, uuid, json
from datetime                       import datetime, date, timedelta

sys.path.append("/flowstacks/public-cloud-src")
from logger.logger import Logger
from modules.base_api.fs_web_tier_base_work_item     import FSWebTierBaseWorkItem
from connectors.redis.redis_pickle_application       import RedisPickleApplication

class RA_DeleteSimpleUserValue(FSWebTierBaseWorkItem):

    def __init__(self, json_data):
        FSWebTierBaseWorkItem.__init__(self, "RA_DSUV", json_data)

        """ Constructor Serialization taking HTTP Post-ed JSON into Python members """
        # Define Inputs and Outputs for the Job to serialize over HTTP
        try:

            # INPUTS:
            self.m_storage_key_id                   = str(json_data["Storage Key ID"])

            # OUTPUTS:
            self.m_results["Status"]                = "FAILED"
            self.m_results["Error"]                 = ""
            self.m_results["Stored Value"]          = ""
            self.m_results["Storage Key ID"]        = self.m_storage_key_id

            # MEMBERS:
            self.m_debug                            = False
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

        self.handle_deleting_value_in_cache()

        self.lg("Done Startup State(ENDING)", 5)

        return None
    # end of handle_startup

        
    def handle_deleting_value_in_cache(self):

        ra_name = "BLOB"
        self.lg("Deleting Value in Key(" + str(self.m_storage_key_id) + ") RA(" + str(ra_name) + ")", 5)

        stored_msg  = self.get_message_no_block(ra_name, self.m_storage_key_id)
        if stored_msg == None:
            self.m_results["Status"]    = "FAILED"
            self.m_results["Message"]   = "Delete Failed to Find Stored Value in Key(" + str(self.m_storage_key_id) + ")"
            self.lg(self.m_results["Message"], 5)

        else:
            self.lg("Found Stored Value and Deleting It", 5)
            self.m_results["Stored Value"]  = stored_msg["Value To Store"]
            self.m_results["Status"]        = "SUCCESS"
        # end of if there was a stored value

        self.lg("Done Deleting Value in Key", 5)

        return None
    # end of handle_deleting_value_in_cache

    
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

# end of RA_DeleteSimpleUserValue




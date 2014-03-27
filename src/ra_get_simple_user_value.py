import sys, uuid
from datetime                       import datetime, date, timedelta

sys.path.append("/flowstacks/public-cloud-src")
from logger.logger import Logger
from modules.base_api.fs_web_tier_base_work_item     import FSWebTierBaseWorkItem
from connectors.redis.redis_pickle_application       import RedisPickleApplication

class RA_GetSimpleUserValue(FSWebTierBaseWorkItem):

    def __init__(self, json_data):
        FSWebTierBaseWorkItem.__init__(self, "RA_GSUV", json_data)

        # INPUTS:
        self.m_storage_key_id                   = str(json_data["Storage Key ID"])

        # OUTPUTS:
        self.m_results["Status"]                = "FAILED"
        self.m_results["Storage Key ID"]        = self.m_storage_key_id
        self.m_results["Stored Value"]          = "NO VALUE FOUND" 

        # MEMBERS:

    # end of  __init__


###############################################################################
#
# Request Handle Methods
#
###############################################################################


    def handle_startup(self):

        self.lg("Start Handle Startup", 5)

        self.handle_get_user_storage()

        self.lg("Done Startup State(ENDING)", 5)

        return None
    # end of handle_startup

        
    def handle_get_user_storage(self):

        ra_name = "BLOB"
        self.lg("Get US in Key(" + str(self.m_storage_key_id) + ") RA(" + str(ra_name) + ")", 5)

        cached_record_hash = self.get_cached_value_from_key(ra_name, self.m_storage_key_id)

        if "Status" in cached_record_hash:
            self.m_results["Status"]    = cached_record_hash["Status"]
           
        if "Exception" in cached_record_hash and cached_record_hash["Exception"] != "":
            self.lg("ERROR: Get User Storage Encountered Exception(" + cached_record_hash["Exception"] + ")", 0)
            self.m_results["Exception"] = cached_record_hash["Exception"]

        if cached_record_hash["Value"] == None:
            no_value_msg    = "No Stored Value Found in Key(" + str(self.m_storage_key_id) + ")"
            self.lg(no_value_msg, 5)
            self.m_results["Stored Value"]  = no_value_msg

        else:
            self.lg("Found Storage in Key(" + str(self.m_storage_key_id) + ")", 5)
            self.m_results["Stored Value"]  = cached_record_hash["Value"]

        self.lg("Done Finding Storage in Key Status(" + str(self.m_results["Status"]) + ")", 5)

        return None
    # end of handle_get_user_storage

    
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

# end of RA_GetSimpleUserValue




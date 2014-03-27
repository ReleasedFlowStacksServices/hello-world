import sys, uuid
from datetime                       import datetime, date, timedelta

sys.path.append("/flowstacks/public-cloud-src")
from logger.logger import Logger
from modules.base_api.fs_web_tier_base_work_item     import FSWebTierBaseWorkItem
from connectors.redis.redis_pickle_application       import RedisPickleApplication

class RA_UpdateSimpleUserValue(FSWebTierBaseWorkItem):

    def __init__(self, json_data):
        FSWebTierBaseWorkItem.__init__(self, "RA_USUV", json_data)

        # INPUTS:
        self.m_access_token                     = str(json_data["Access Token"])
        self.m_storage_key_id                   = str(json_data["Storage Key ID"])
        self.m_value_to_store                   = str(json_data["Value To Update"])

        # OUTPUTS:
        self.m_results["Status"]                = "FAILED"
        self.m_results["Storage Key ID"]        = self.m_storage_key_id

        # MEMBERS:
        self.m_hash_to_store                    = {}

    # end of  __init__


###############################################################################
#
# Request Handle Methods
#
###############################################################################


    def handle_startup(self):

        self.lg("Start Handle Startup", 5)

        self.handle_updating_value_in_cache()

        self.lg("Done Startup State(ENDING)", 5)

        return None
    # end of handle_startup

        
    def handle_updating_value_in_cache(self):

        ra_name = "BLOB"
        self.lg("Updating Value in Key(" + str(self.m_storage_key_id) + ") RA(" + str(ra_name) + ")", 5)

        stored_msg  = self.get_message_no_block(ra_name, self.m_storage_key_id)
        if stored_msg == None:
            self.m_results["Status"]    = "FAILED"
            self.m_results["Message"]   = "Update Failed to Find Stored Value in Key(" + str(self.m_storage_key_id) + ")"
            self.lg(self.m_results["Message"], 5)

        else:
            self.lg("Found Stored Value Updating It", 5)
            self.m_hash_to_store = {
                                "Access Token"          : self.m_access_token,
                                "Job ID"                : stored_msg["Job ID"],
                                "Value To Store"        : self.m_value_to_store
            }
            self.put_into_app_key(ra_name, self.m_storage_key_id, self.m_hash_to_store)
            self.m_results["Status"]    = "SUCCESS"
        # end of if there was a stored value

        self.lg("Done Updating in Key", 5)

        return None
    # end of handle_updating_value_in_cache

    
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

# end of RA_UpdateSimpleUserValue



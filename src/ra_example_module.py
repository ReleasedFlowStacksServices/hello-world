import sys, uuid
from datetime                       import datetime, date, timedelta

sys.path.append("/flowstacks/public-cloud-src")
from logger.logger import Logger
from modules.base_api.fs_web_tier_base_work_item     import FSWebTierBaseWorkItem
from connectors.redis.redis_pickle_application       import RedisPickleApplication

# Rest API Example Module:
# Out of convention FlowStacks REST API Modules begin with "RA_"
class RA_ExampleModule(FSWebTierBaseWorkItem):

    def __init__(self, json_data):
        # The RA_ExampleModule becomes the prefix for the Module when it is logged to the system
        FSWebTierBaseWorkItem.__init__(self, "RA_ExampleModule", json_data)

        # INPUTS:
        self.m_input_key                        = json_data["This is an Input Key"]

        # OUTPUTS:
        self.m_results["Status"]                = "FAILED"
        self.m_results["This is an Output Key"] = "NO OUTPUT"

        # MEMBERS:
        self.m_debug                            = False

    # end of  __init__


###############################################################################
#
# Job Module Handle Each State Methods
#
###############################################################################


    def handle_startup(self):

        self.lg("Start Handle Module Startup", 5)

        self.m_state                = "Results"
        self.m_results["Status"]    = "FAILED"

        self.lg("Done Module Startup State(" + self.m_state + ")", 5)

        return None
    # end of handle_startup

    
    def handle_processing_results(self):

        self.lg("Processing Results", 5)

        # For this Example just show the Input can be set to the Output for testing
        self.m_results["Status"]                = "SUCCESS"
        self.m_results["This is an Output Key"] = str(self.m_input_key)

        self.lg("Done Processing Results", 5)

        return None
    # end of handle_processing_results


###############################################################################
#
# Helpers
#
###############################################################################


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

# end of RA_ExampleModule



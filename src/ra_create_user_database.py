import sys, uuid
from datetime                       import datetime, date, timedelta

sys.path.append("/flowstacks/public-cloud-src")
from logger.logger import Logger
from modules.base_api.fs_web_tier_base_work_item     import FSWebTierBaseWorkItem
from connectors.redis.redis_pickle_application       import RedisPickleApplication


class RA_FlowStacks_CreateUserDatabase(FSWebTierBaseWorkItem):

    def __init__(self, json_data):
        FSWebTierBaseWorkItem.__init__(self, "RA_FlowStacks_CreateUserDatabase", json_data)

        # INPUTS:
        self.m_db_host_endpoint                 = json_data["Database Host Endpoint"]
        self.m_db_port                          = json_data["Database Port"]
        self.m_db_name                          = json_data["Database Name"]
        self.m_db_type                          = json_data["Database Type"]
        self.m_db_debug                         = json_data["Database Debug"]
        self.m_db_auto_commit                   = json_data["Database Auto Commit"] == "True"
        self.m_db_auto_flush                    = json_data["Database Auto Flush"] == "True"
        self.m_db_FS_user_name                  = json_data["Database FlowStacks User Name"]
        self.m_db_FS_user_password              = json_data["Database FlowStacks User Password"]
        self.m_db_user_name                     = json_data["Database New User Name"]
        self.m_db_user_password                 = json_data["Database New User Password"]

        # OUTPUTS:
        self.m_results["Status"]                = "FAILED"
        self.m_results["Error"]                 = ""

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

        error_msg                   = ""
        self.m_state                = "Results"

        try:

            import sqlalchemy, os

            connection_str  = str("mysql://" + str(self.m_db_FS_user_name) + ":" + str(self.m_db_FS_user_password) + "@" + str(self.m_db_host_endpoint))

            self.lg("Connecting to DB", 5)
            error_msg   = "Connecting to Database Failed"
            engine      = sqlalchemy.create_engine(connection_str)

            error_msg   = "Creating New Database Failed"
            new_db      = "CREATE DATABASE IF NOT EXISTS " + str(self.m_db_name) + ";"
            self.lg("Creating New DB(" + str(self.m_db_name) + ")", 5)
            engine.execute(new_db)

            error_msg   = "Creating New User Failed"
            new_user    = "mysql -u" + str(self.m_db_FS_user_name) + " -p'" + str(self.m_db_FS_user_password) + "' -h" + str(self.m_db_host_endpoint) + " -e \"CREATE USER '" + str(self.m_db_user_name) + "'@'localhost' IDENTIFIED BY '" + str(self.m_db_user_password) + "'; CREATE USER '" + str(self.m_db_user_name) + "'@'%' IDENTIFIED BY '" + str(self.m_db_user_password) + "';\""
            self.lg("Creating New User(" + str(new_user) + ")", 5)
            os.system(new_user)

            error_msg   = "Granting User Database Privileges Failed"
            new_grant   = "GRANT ALL ON `" + str(self.m_db_name) + "`.* TO '" + str(self.m_db_user_name) + "'@'localhost';"
            self.lg("Granting User DB Privs(" + str(new_grant) + ")", 5)
            engine.execute(new_grant)

            error_msg   = "Granting User Database Privileges Failed"
            new_grant   = "mysql -u" + str(self.m_db_FS_user_name) + " -p'" + str(self.m_db_FS_user_password) + "' -h" + str(self.m_db_host_endpoint) + " -e \"GRANT ALL ON \`" + str(self.m_db_name) + "\`.* TO '" + str(self.m_db_user_name) + "'@'%';\""
            self.lg("Granting User DB perc Privs", 5)
            os.system(new_grant)

            error_msg   = "Granting User Database Privileges Failed"
            new_grant   = "GRANT ALL ON `" + str(self.m_db_name) + "`.* TO '" + str(self.m_db_user_name) + "'@'" + str(self.m_db_name) + "';"
            self.lg("Granting User(" + str(new_grant) + ")", 5)
            engine.execute(new_grant)

            error_msg   = "Flushing Privileges Failed"
            flush_privs = "FLUSH PRIVILEGES;"
            self.lg("Flushing(" + str(flush_privs) + ")", 5)
            engine.execute(flush_privs)

            self.m_results["Status"]    = "SUCCESS"
            self.m_results["Error"]     = ""

        except Exception,e:

            self.lg("Creating New User Database Failed Error(" + str(error_msg) + ") Exception(" + str(e) + ")", 5)
            self.m_results["Status"]    = "FAILED"
            self.m_results["Error"]     = error_msg


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

# end of RA_FlowStacks_CreateUserDatabase



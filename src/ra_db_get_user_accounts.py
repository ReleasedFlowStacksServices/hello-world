import sys, uuid
from datetime                       import datetime, date, timedelta

sys.path.append("/flowstacks/public-cloud-src")
from logger.logger import Logger
from modules.base_api.fs_web_tier_base_work_item    import FSWebTierBaseWorkItem
from connectors.redis.redis_pickle_application      import RedisPickleApplication

# Workflow Schema File:
from demo_database_schema                           import LT_UserStatus, PT_UserAccount


class RA_DB_GetUserAccounts(FSWebTierBaseWorkItem):

    def __init__(self, json_data):
        FSWebTierBaseWorkItem.__init__(self, "RA_FB_DB_GUA", json_data)

        try:
            # INPUTS:
            self.m_db_connection_str            = str(json_data["DB Conn Str"])
            self.m_query_type                   = str(json_data["Query Type"])

            self.m_first_name                   = ""
            self.m_last_name                    = ""
            self.m_user_name                    = ""
            self.m_user_status_id               = int(0)
            self.m_password                     = ""
            self.m_email                        = ""

            if "First Name" in json_data:
                self.m_first_name               = str(json_data["First Name"])
        
            if "Last Name" in json_data:
                self.m_last_name                = str(json_data["Last Name"])
        
            if "Password" in json_data:
                self.m_password                 = str(json_data["Password"])
            
            if "Email" in json_data:
                self.m_email                    = str(json_data["Email"])
        
            if "User Name" in json_data:
                self.m_user_name                = str(json_data["User Name"])
        
            if "User Status ID" in json_data:
                self.m_user_status_id           = int(json_data["User Status ID"])
        
            # OUTPUTS:
            self.m_results["Status"]            = "FAILED"
            self.m_results["Error"]             = ""
            self.m_results["Records"]           = []

            # MEMBERS:
            self.m_db_records                   = []
            self.m_debug                        = False

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

            import sqlalchemy
            from sqlalchemy import Column, Integer, String, ForeignKey, Table, create_engine, MetaData, Date, DateTime, Float, Boolean
            from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker, relation
            from sqlalchemy.ext.declarative import declarative_base

            self.lg("Connecting to User DB(" + str(self.m_db_connection_str) + ")", 5)
            error_msg   = "Creating Engine Base Failed"
            engine      = sqlalchemy.create_engine(self.m_db_connection_str)

            error_msg   = "Connecting Engine Failed"
            connection  = engine.connect()
            error_msg   = "Creating Scoped Session Failed"
            session     = scoped_session(sessionmaker(autocommit=False,
                                                      autoflush=False,
                                                      bind=engine))

            self.execute_query(session)
                
            self.m_results["Status"]    = "SUCCESS"
            self.m_results["Error"]     = ""

        except Exception,e:

            self.lg("Getting Developer Accounts Failed Error(" + str(error_msg) + ") Exception(" + str(e) + ")", 5)
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


    def execute_query(self, session):

        self.lg("Execute Query", 5)

        import sqlalchemy
        import sqlalchemy.orm
        from sqlalchemy import Column, Integer, String, ForeignKey, Table, create_engine, MetaData, Date, DateTime, Float, cast, or_, and_
        from sqlalchemy.exc import OperationalError
        try:

            if self.m_query_type == "Get By User Name":
                self.m_db_records   = session.query(PT_UserAccount).filter(PT_UserAccount.user_name==self.m_user_name).all()

            else:
                error_msg                   = "Unsupported Query(" + str(self.m_query_type) + ")"
                self.lg("ERROR(" + str(error_msg) + ")", 5)
                self.m_results["Status"]    = "FAILED"
                self.m_results["Error"]     = error_msg

            self.convert_all_found_db_records()

        # Make sure to account for the completely new DB 
        except Exception, e:
            error_msg                   = "Exception During Query(" + str(self.m_query_type) + ")"
            self.lg("ERROR(" + str(error_msg) + ") Exception(" + str(e) + ")", 5)
            self.m_results["Status"]    = "FAILED"
            self.m_results["Error"]     = error_msg

        self.lg("Done Execute Query", 5)

        return None
    # end of execute_query


    def convert_all_found_db_records(self):
        
        self.lg("Start Convert DB Records(" + str(len(self.m_db_records)) + ")", 5)

        for record in self.m_db_records:
            new_dict    = self.convert_db_to_dict(record)
            self.m_results["Records"].append(new_dict)

        self.lg("Done Convert DB Records", 5)

        return None
    # end of convert_db_records

            
    def convert_db_to_dict(self, record):

        new_dict    = {}
        
        self.lg("Convert Record", 5)

        new_dict["First Name"]          = record.first_name
        new_dict["Last Name"]           = record.last_name
        new_dict["Email"]               = record.email
        new_dict["Creation Date"]       = record.creation_date.strftime("%Y-%m-%d")
        new_dict["User Name"]           = record.user_name
        new_dict["User Status ID"]      = record.user_status_sym  

        self.lg("Done Convert Record", 5)

        return new_dict
    # end of convert_db_to_dict
    

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

# end of RA_FlowStacks_DB_GetDeveloperAccount



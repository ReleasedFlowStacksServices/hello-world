import sys, uuid, json
from datetime                       import datetime, date, timedelta

sys.path.append("/flowstacks/public-cloud-src")
from logger.logger import Logger
from modules.base_api.fs_web_tier_base_work_item     import FSWebTierBaseWorkItem
from connectors.redis.redis_pickle_application       import RedisPickleApplication


class RA_FlowStacks_ApplySchemaChanges(FSWebTierBaseWorkItem):

    def __init__(self, json_data):
        FSWebTierBaseWorkItem.__init__(self, "RA_FlowStacks_ApplySchemaChanges", json_data)

        """ Constructor Serialization taking HTTP Post-ed JSON into Python members """
        # Define Inputs and Outputs for the Job to serialize over HTTP
        try:

            # INPUTS:
            self.m_project_schema_file              = str(json_data["Schema File"])
            self.m_db_host_endpoint                 = str(json_data["Database Host Endpoint"])
            self.m_db_port                          = str(json_data["Database Port"])
            self.m_db_name                          = str(json_data["Database Name"])
            self.m_db_type                          = str(json_data["Database Type"])
            self.m_db_debug                         = str(json_data["Database Debug"])
            self.m_db_auto_commit                   = bool(json_data["Database Auto Commit"] == "True")
            self.m_db_auto_flush                    = bool(json_data["Database Auto Flush"] == "True")
            self.m_db_user_name                     = str(json_data["Database New User Name"])
            self.m_db_user_password                 = str(json_data["Database New User Password"])

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
# Job Module Handle Each State Methods
#
###############################################################################


    def handle_startup(self):

        self.lg("Start Handle Module Startup", 5)

        error_msg                   = ""
        path_to_schema_file         = self.m_project_schema_file.replace(".py", '')
        self.m_state                = "Results"
        last_class_name             = "No Class Loaded Yet"
        
        try:

            import sqlalchemy
            from sqlalchemy import Column, Integer, String, ForeignKey, Table, create_engine, MetaData, Date, DateTime, Float, Boolean
            from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker, relation
            from sqlalchemy.ext.declarative import declarative_base

            connection_str  = str("mysql://" + str(self.m_db_user_name) + ":" + str(self.m_db_user_password) + "@" + str(self.m_db_host_endpoint) + ":" + str(self.m_db_port) + "/" + str(self.m_db_name))

            self.lg("Connecting to User DB(" + str(connection_str) + ")", 5)
            error_msg   = "Creating Engine Base"
            engine      = sqlalchemy.create_engine(connection_str)

            error_msg   = "Connecting Engine"
            connection  = engine.connect()
            error_msg   = "Creating Scoped Session"
            session     = scoped_session(sessionmaker(autocommit=self.m_db_auto_commit,
                                                      autoflush=self.m_db_auto_flush,
                                                      bind=engine))
            
            try:
                import inspect
                error_msg       = "Walking through Schema File(" + str(path_to_schema_file) + ")"
                Base            = declarative_base()
                new_module      = __import__(path_to_schema_file)
                the_job_module  = None
                classes_to_add  = []
                for name, job_module_obj in inspect.getmembers(new_module):
                    if inspect.isclass(job_module_obj) and (str(job_module_obj.__class__.__name__) == "DeclarativeMeta") and name != "Base":
                        last_class_name = str(job_module_obj.__class__.__name__)
                        self.lg("Class DB Name(" + str(last_class_name) + ")", 5)
                        job_module_obj.__table__.create(engine, checkfirst=True)

                self.lg("Found Schema Classes(" + str(len(classes_to_add)) + ")", 5)

                error_msg   = "Commiting All Schema Classes"
                session.commit()

            except Exception,f:
                self.m_results["Status"]    = "FAILED"
                self.m_results["Error"]     = "Applying DB Schema Loading had Exception(" + str(f) + ") with Last Class(" + str(last_class_name) + ")"
                return None

            self.m_results["Status"]    = "SUCCESS"
            self.m_results["Error"]     = ""

        except Exception,e:

            self.lg("Applying New User Database Failed Error(" + str(error_msg) + ") Exception(" + str(e) + ")", 5)
            self.m_results["Status"]    = "FAILED"
            self.m_results["Error"]     = "Applying DB Schema had Exception(" + str(f) + ") with Last Class(" + str(last_class_name) + ")"


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



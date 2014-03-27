import sys, uuid, json
from datetime                       import datetime, date, timedelta

sys.path.append("/flowstacks/public-cloud-src")
from logger.logger import Logger
from modules.base_api.fs_web_tier_base_work_item    import FSWebTierBaseWorkItem
from connectors.redis.redis_pickle_application      import RedisPickleApplication

# Workflow Schema File:
from demo_database_schema                           import LT_UserStatus, PT_UserAccount


class RA_FlowStacks_DB_CreateNewRecord(FSWebTierBaseWorkItem):

    def __init__(self, json_data):
        FSWebTierBaseWorkItem.__init__(self, "RA_FS_DB_CNR", json_data)

        try:
            # INPUTS:
            self.m_db_connection_str                = str(json_data["DB Conn Str"])
            self.m_project_schema_file              = str(json_data["Schema File"])
            self.m_schema_class                     = str(json_data["Schema Class Name"])
            self.m_schema_field_data                = json_data["Schema Field Data"]
            self.m_sql_meta                         = None
            
            # OUTPUTS:
            self.m_results["Status"]            = "FAILED"
            self.m_results["Error"]             = ""

            # MEMBERS:
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
            from sqlalchemy.exc import ProgrammingError
            from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker, relation

            error_msg   = "Creating Engine Base Failed"
            engine      = sqlalchemy.create_engine(self.m_db_connection_str)

            error_msg   = "Connecting Engine to DB(" + str(self.m_db_connection_str) + ") Failed"
            connection  = engine.connect()

            self.m_sql_meta     = MetaData(bind=engine)
            self.m_sql_meta.reflect()

            error_msg   = "Creating Scoped Session Failed"
            session     = scoped_session(sessionmaker(autocommit=False,
                                                      autoflush=False,
                                                      bind=engine))

            error_msg   = "Creating New Record Failed"
            new_obj     = self.create_new_object()
                
            error_msg   = "Adding New Record Failed"
            session.add(new_obj)
            self.lg("Commiting New Record", 5)

            error_msg   = "Commiting All Schema Classes"
            session.commit()
    
            self.m_results["Status"]    = "SUCCESS"
            self.m_results["Error"]     = ""

            # end of new creation case

        except Exception,e:

            self.lg("Creating New Record Failed Error(" + str(error_msg) + ") Exception(" + str(e) + ")", 5)
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


    def create_new_object(self):
        
        self.lg("Creating New Object(" + str(self.m_schema_class) + ") Path(schema." + str(self.m_project_schema_file) + ")", 5)

        schema_class_name = self.m_schema_class
        module_filename = self.m_project_schema_file
        fields = self.m_schema_field_data
        
        module_name = module_filename.split('.')[0]
        module = getattr(__import__('schema.' + module_name), module_name)
        obj_class = getattr(module, schema_class_name)
        obj = obj_class()

        import sqlalchemy
        from sqlalchemy import Column, Integer, String, ForeignKey, Table, create_engine, MetaData, Date, DateTime, Float, Boolean
        from sqlalchemy.orm import relationship, backref, scoped_session, sessionmaker, relation
        from sqlalchemy.ext.declarative import declarative_base

        # Find the DB Meta Object from the SqlAlchemy Schema 
        table   = Table(str(self.m_sql_meta.tables[obj.__table__.name]), self.m_sql_meta)
        columns = table.columns.keys()
        self.lg("Log Output Demo(" + str(len(columns)) + ")", 5)
        for column in columns:

            if column != "id":

                column_type = str(table.columns[column].type)

                test_if_present = False
                if column in fields:
                    test_if_present = True
                    self.lg("Setting(" + str(column) + ") to Value(" + str(fields[column]) + ")", 5)
                else:
                    self.lg("Setting(" + str(column) + ") to Default Type(" + str(column_type) + ")", 5)

                if "INTEGER"    in column_type:
                    if test_if_present:
                        setattr(obj, column, int(fields[column]))
                    else:
                        setattr(obj, column, int(0))

                elif "FLOAT"    in column_type:
                    if test_if_present:
                        setattr(obj, column, float(fields[column]))
                    else:
                        setattr(obj, column, float(0.0))

                elif "VARCHAR"  in column_type:
                    if test_if_present:
                        setattr(obj, column, str(fields[column]))
                    else:
                        setattr(obj, column, str(""))

                elif "DATETIME" in column_type:
                    if test_if_present:
                        setattr(obj, column, datetime.strptime(fields[column], '%b %d %Y %I:%M%p'))
                    else:
                        setattr(obj, column, datetime.now())

                elif "DATE"     in column_type:
                    if test_if_present:
                        setattr(obj, column, datetime.strptime(fields[column], '%b %d %Y %I:%M%p'))
                    else:
                        setattr(obj, column, datetime.now())
                
                else:
                    self.lg("ERROR: Missing SQLALCHEMY TRANSLATOR TYPE(" + str(column) + ") DEFAULTING TO NONE", 5)
                    setattr(obj, column, None)

        # SqlAlchemy dynamic mapping

        self.lg("Done Creating New Object", 5)

        return obj
    # end of create_new_object

    
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

# end of RA_FlowStacks_DB_CreateNewRecord



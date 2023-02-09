import os
import sqlite3
import logging
from utility.dateTime import parse_date, get_current_datetime_string as currentDateTime
from utility.util import determine_file_type, get_file_hash as fileHash, get_configurations as getConfig 
import threading
import queue

class HashCheck:

    def __init__(self, config_path="../default_config.json", db_file_path=None):
        # Load configuration from the given path
        self.config = getConfig(config_path, 'hash')
        # Get the log file path and name from the config file and setup logger
        self.log_file = os.path.join(self.config.get('logFolderParentFolderPath', None), self.config.get('logFileName', None))

        self.logger = logging.getLogger(__name__)
        self._configure_logger()
        self.logger.debug('Log File Path: {0}'.format(self.log_file))

        self.root_directories = self.config.get('rootFolderList', [])
        self.exclusions = self.config.get('exclusions',{})
        self.db_file_name = self.config.get('dbFile','hash.db')
        self.db_folder_path = self.config.get('dbFileParentFolderPath','./')
        self.root_dir = None

        self.directoryQueue = queue.Queue()

    def _configure_logger(self):
        # dump all log levels to file
        log_level = self.config.get('logLevel', "INFO")

        numeric_log_level = getattr(logging, log_level.upper(), None)
        if not isinstance(numeric_log_level, int):
            raise ValueError("Invalid log level: %s" % log_level)

        self.logger.setLevel(numeric_log_level)

        # create a file handler to log to a file
        file_handler = None

        # Check if we should append to the file or write a new file
        if self.config.get('singleFileLog', False):
            file_handler = logging.FileHandler(self.log_file, mode='a')
        else:
            file_handler = logging.FileHandler(currentDateTime() + ' ' + self.log_file , mode='w')

        file_handler.setLevel(numeric_log_level)

        # create a console handler to log to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # create a formatter for the logs
        file_formatter = logging.Formatter('%(asctime)s - %(process)d - %(thread)d - %(name)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(asctime)s - %(process)d - %(thread)d - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)
        # add the file handler to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def create_db(self):
        """
        Creates the 'file_db.db' file in the root directory and creates the
        'files' table.
        """
        dbFilePath = os.path.join(self.db_folder_path, self.db_file_name)
        conn = None
        # check if the database file already exists
        if os.path.exists(dbFilePath):
            self.logger.debug(f"The database file already exists at {dbFilePath}")
            return
        
        self.logger.info(f"Creating a new database file at {dbFilePath}")
        
        # create the database file and table
        conn = sqlite3.connect(dbFilePath)
        cursor = conn.cursor()
        
        # Read the schema file
        try:
            with open("hashCheck/hash_db_schema.sql", "r") as f:
                schema = f.read()
        except Exception as e:
            self.logger.error(f"Error reading schema file: {e}")
            return
        
        # Execute the SQL commands in the schema file
        try:
            cursor.executescript(schema)
        except Exception as e:
            self.logger.error(f"Error executing schema script: {e}")
            return
        
        conn.commit()
        self.logger.info(f"Database file created successfully at {dbFilePath}")
        return conn

    def scan_and_hash_files(self, directories = None):
        """
        Loops through all root directories

        Scans all files in the root directory and nested subdirectories, 
        gets their hashes, and saves the information to db.
        """

        # Create the directory queue
        self.directoryQueue = queue.Queue()

        # If no directory paths are in the config exit
        if self.root_directories:
            for root_path in self.root_directories:
                self.directoryQueue.put(root_path)
        elif directories:
            for root_path in directories:
                self.directoryQueue.put(root_path)
        else:
            return

        # Creates a DB with the name provided, if one is not found at the file path
        self.create_db()


        # Start the directory workers
        threads = []
        for i in range(15):
            t = threading.Thread(target=self.directory_worker)
            t.start()
            threads.append(t)

        # wait for all items to be processed
        self.directoryQueue.join()
        for t in threads:
            t.join()   

    def directory_worker(self):
        """
            Worker keeps grabbing directories from the queue, if none are found it waits 60 seconds and terminates
        """
        path = None
        while True:
            try:
                # Get the next item from the queue
                path = self.directoryQueue.get(timeout=60)
            except Exception as e:
                break
            
            # Scan Hash files in directory
            self._scan_and_hash_files(path)

            # Let the queue know the task has be finished
            self.directoryQueue.task_done()
    
    def _crud_db(self, conn, db_actions):
        # Bulk Inserting Updated and Deleting from the database

        self._delete_file_record(conn, db_actions["delete_file_record"])
        self._clear_missing_date(conn, db_actions["clear_missing_date"])
        self._update_missing_date(conn, db_actions["update_missing_date"])
        self._update_mismatch_date(conn, db_actions["update_mismatch_date"])
        self._clear_mismatch_date(conn, db_actions["clear_mismatch_date"])
        self._insert_file_record(conn, db_actions["insert_file_record"])
    
    def _get_db_actions_skeleton(self):
        return {
            "clear_missing_date" : [],
            "update_missing_date" : [],
            "update_mismatch_date" : [],
            "clear_mismatch_date" : [],
            "insert_file_record" : [],
            "delete_file_record" : [],
        }

    def _scan_and_hash_files(self, path):
        """
        Scans all files in the root directory and nested subdirectories, 
        gets their hashes, and saves the information to db.
        """
        conn = self.connect_db()
        # Check if root directory is specified and exists
        if not path or not os.path.exists(path):
            self.logger.error('path not found')
            return

        # Create a list of transactions the db needs to do, so that the db is not bogged down by constant transactions
        db_action_lists = self._get_db_actions_skeleton()

        # Scan directory and loop through all files and folders
        with os.scandir(path) as items:
            for item in items:
                if item.is_dir():
                    # check if directory on exclusion list
                    if self._is_directory_excluded(item):
                        continue

                    # Add to queue for another worker to process
                    self.directoryQueue.put(os.path.join(path, item))
                elif item.is_file():
                    # Get the full file path
                    file_path = os.path.join(path, item)

                    # Log the current file being processed
                    self.logger.debug(f'Processing file: {file_path}')

                    # Process the file
                    self._process_file(item.name, file_path, conn, db_action_lists) 
                
        # Only update the DB if there are transactions that need to process
        if not self._is_empty_actions(db_action_lists):
            self._crud_db(conn, db_action_lists)

        # Commit changes and close the database connection
        conn.commit()
        self.logger.debug('Committed changes to the database')
        conn.close()
        self.logger.debug('Closed database connection')

    def _is_empty_actions(self, actions):
        if not actions.get("clear_missing_date", None) and not actions.get("update_mismatch_date", None) and not actions.get("clear_mismatch_date", None) and not actions.get("insert_file_record", None) and not actions.get("delete_file_record", None):
            return True
        return False

    def _process_file(self,file,file_path, conn, db_action):
        if self._skip_file(file, file_path):
            return

        # Check if the file is already in the database
        result = self._check_existing_file_in_db(conn,file_path)
        # Check if the file still exists
        if os.path.exists(file_path):
            # Get the file's hash
            file_hash = fileHash(file_path)

            if result: 
                self.logger.debug("File found in database")
                missing_date = result[3]
                hash_value = result[1]
                mismatch_date = result[4]
                self.logger.debug(f"missing_date: {missing_date}, hash_value: {hash_value}, mismatch_date: {mismatch_date}")
                # Clear missing date if exists
                if missing_date:
                    self.logger.info(f"Clearing existing missing date for  {file_path}")
                    db_action["clear_missing_date"].append(file_path)

                # Check if the hash has changed
                if file_hash != hash_value:
                    # update the mismatch date
                    self.logger.info(f'Hash mismatch for {file_path}')
                    db_action["update_mismatch_date"].append(file_path)
                else:
                    if mismatch_date:
                        # Clear the mismatch date
                        self.logger.info(f'Clearing mismatch date for {file_path}')
                        db_action["clear_mismatch_date"].append(file_path)
            else:
                self.logger.info(f'New file added {file_path}')
                db_action["insert_file_record"].append((file_path, file_hash))
                
        else:
            # If File is missing
            if result:
                # Update the database with the missing date
                self.logger.info(f'File missing for {file_path}')
                db_action["update_missing_date"].append(file_path)

    def _skip_file(self, file, file_path):
        """
        Check if file should be skipped.
        """
        # Check if file extension is in the exclusion list
        if any(file.endswith(ext) for ext in self.exclusions.get("extensions", [])):
            self.logger.debug(f"Skipping file {file}. File extension is in exclusion list.")
            return True
        # Check if file name is in the exclusion list
        if file in self.exclusions.get("fileNames", []):
            self.logger.debug(f"Skipping file {file}. File name is in exclusion list.")
            return True
        # Check if file path is in the exclusion list
        for path in self.exclusions.get("paths", []):
            if os.path.commonpath([path, file_path]) == path:
                self.logger.debug(f"Skipping file {file}. File path is in exclusion list.")
                return True
        # If no exclusions apply, file is not skipped
        self.logger.debug(f"File {file} is not in the exclusion list.")
        return False

    def _is_directory_excluded(self,item):
        """
        Check if directory should be skipped
        """
        if item.name in self.exclusions.get("folderNames",[]):
            return True
        
        return False

    def get_flagged_files(self, flag = None):
        """
        Returns the records that have either missing or mismatched dates based on the flag passed in. 
        flag = 'missing' or 'mismatch'
        """
        conn = self.connect_db()
        cursor = conn.cursor()
        # Log the start of the function with an info log
        self.logger.info("Getting flagged files from database")

        # Get the records based on the flag
        if flag == "missing":
            cursor.execute("SELECT * FROM files WHERE missing_date IS NOT NULL")
        elif flag == "mismatch":
            cursor.execute("SELECT * FROM files WHERE mismatch_date IS NOT NULL")
        elif flag == None:
            cursor.execute("SELECT * FROM files WHERE mismatch_date IS NOT NULL OR missing_date IS NOT NULL")
        else:
            # Log an error if an invalid flag is passed in
            self.logger.error("Invalid flag. Please use 'missing' or 'mismatch'.")
            raise ValueError("Invalid flag. Please use 'missing' or 'mismatch'.")
            
        results = cursor.fetchall()
        
        # Log the number of results retrieved
        self.logger.debug(f"{len(results)} results retrieved")
        
        conn.close()

        report = self._get_report(results)
        return report

    def get_all_files(self):
        """
        Returns all files in the database
        """
        conn = self.connect_db()
        cursor = conn.cursor()

        # Check if database is connected
        if not conn:
            self.logger.error("Failed to connect to database")
            return

        self.logger.debug("Fetching all files from database")
        cursor.execute("SELECT * FROM files")
        results = cursor.fetchall()
        conn.close()

        # Log the result of the query execution
        if results:
            self.logger.info(f"Found {len(results)} files")
        else:
            self.logger.warning(f"No files found")

        report = self._get_report(results)

        return report

    def custom_query_execute(self, query):
        """
        Executes a custom query and returns the results as a report (json format)
        """
        # Connect to the database and return if the connection was unsuccessful
        conn = self.connect_db
        cursor = conn.cursor()
        
        # Log the start of the function execution
        self.logger.debug(f"Executing query: {query}")
        results = None
        # Execute the query to retrieve the file records
        try:
            cursor.execute(query)
            results = cursor.fetchall()
        except Exception as e:
            self.logger.error(f'An error occured when trying to execute the following query: {query} \n Error: {e}')
        
        # Log the result of the query execution
        if results:
            self.logger.info(f"Found {len(results)} files of type {file_type}")
        else:
            self.logger.warning(f"No files found of type: {file_type}")
        
        # Call the function to get the report from the results
        report = self._get_report(results)
        conn.close()
        # Return the report
        return report

    def get_files_by_type(self, file_type):
        """
        Returns the records that have the file type that is passed in
        """
        # Connect to the database and return if the connection was unsuccessful
        conn = self.connect_db()
        cursor = conn.cursor()
        # Log the start of the function execution
        self.logger.debug(f"Getting files of type: {file_type}")
        
        # Execute the query to retrieve the file records
        cursor.execute("SELECT * FROM files WHERE file_type=?", (file_type,))
        results = cursor.fetchall()
        
        # Log the result of the query execution
        if results:
            self.logger.info(f"Found {len(results)} files of type {file_type}")
        else:
            self.logger.warning(f"No files found of type: {file_type}")
        
        # Call the function to get the report from the results
        report = self._get_report(results)
        conn.close()
        # Return the report
        return report

    def get_files_by_initial_date(self,initial_date):
        """
        Retrieve records that match the initial date based on a date or datetime that is passed in as an argument.
        """
        # Log a debug message indicating the start of the function
        self.logger.debug("Getting files by initial date: %s", initial_date)

        # Check if the database connection is established
        conn = self.connect_db()
        cursor = conn.cursor()

        # Log an info message indicating the parsing of the initial date
        self.logger.info("Parsing initial date: %s", initial_date)

        # Check if the passed-in argument is a string and parse it to a datetime if it is
        if isinstance(initial_date, str):
            initial_date = parse_date(initial_date)

        # Log a debug message indicating the execution of the SQL query
        self.logger.debug("Executing SQL query: SELECT * FROM files WHERE initial_date = %s", initial_date)

        # Execute the SQL query to retrieve the matching records
        cursor.execute("SELECT * FROM files WHERE initial_date = ?", (initial_date,))
        results = self.cursor.fetchall()
        conn.close()

        # Log a debug message indicating the start of the `_get_report` function
        self.logger.debug("Getting report from results")

        # Call the `_get_report` function to generate a report from the results
        report = self._get_report(results)

        # Log an info message indicating the successful completion of the function
        self.logger.info("Successfully retrieved files by initial date: %s", initial_date)

        return report

    def reinitialize_db(self):
        """
        Resets all the fields of the db and re-scan all files
        """
        conn = self.connect_db()
        cursor = conn.cursor()

        try:
            cursor.execute("DELETE FROM files")
            conn.commit()
            self.logger.info("Successfully deleted all records from the database")
        except Exception as e:
            self.logger.error("An error occurred while deleting the records from the database: {}".format(e))
            return
        conn.close()
        self.logger.info("Starting to re-scan all the files")
        self.scan_and_hash_files()

    def reinitialize_specific(self, scan_list):
        """
        Re-scans specified folders or files and updates their hash and initial date values in the database.

        scan_list should include full paths to the folder or files
        """

        db_actions = self._get_db_actions_skeleton()
        valid_directories = set()
        # Walk through all files and directories in the scan list
        for item in scan_list:
            if not os.path.exists(item):
                continue

            if os.path.isdir(item):
                for subdir, dirs, files in os.walk(item):
                    # Skip any directories in the skip list
                    dirs = self._skip_directories(dirs)
                    self.logger.debug("Removed directories from iteration")
                    
                    valid_directories.add(item.path)

                    for file in files:
                        # Get the full file path
                        file_path = subdir + os.sep + file

                        # Skip files that are in the exclusion list
                        if self._skip_file(file,file_path):
                            self.logger.debug("File %s is in the exclusion list and will be skipped", file)
                            continue

                        # Re-initialize the record with the file path
                        db_actions["delete_file_record"].append(file_path)
                        self.logger.info("Record for file %s has been re-initialized", file)
            elif os.path.isfile(item):
                # Re-initialize the record with the file path
                valid_directories.add(os.path.dirname(item))
                db_actions["delete_file_record"].append(item.path)
                self.logger.info("Record for file %s has been re-initialized", item)
            else:
                self.logger.warning("%s is not a valid file or directory. Skipping...", item)
        
        conn = self.connect_db
        self._crud_db(conn, db_actions)
        conn.close()

        self.scan_and_hash_files(list(valid_directories))

    def connect_db(self, dbFilePath = None):
        '''
        Connects to the sqlite database. If it doesn't exist it creates a new db
        '''
        conn = None
        if not dbFilePath:
            dbFilePath = os.path.join(self.db_folder_path, self.db_file_name)

        # Check if the database file already exists
        if not os.path.exists(dbFilePath):
            self.logger.info("Creating database since it doesn't exist")
            conn = self.create_db()
        try:
            conn = sqlite3.connect(dbFilePath)
            self.logger.info("Successfully connected to database")
        except Exception as e:
            self.logger.error("Error connecting to database: %s", e)
            raise Exception("Error connecting to database")

        if conn:
            cursor = conn.cursor()
            self.logger.debug("Cursor created successfully")
        else:
            self.logger.error("Database connection was not established")
            raise Exception("Database connection was not established")
        return conn

    def _get_report(self, db_results):
        '''
        Returns a json object containing the results of a query
        '''
        report = {}

        # Loop through each record in the database results
        for row in db_results:
            file_path, file_hash, initial_date, missing_date, mismatch_date, file_type = row

            # Add each record to the report dictionary
            report[file_path] = {
                                "file_path" : file_path, 
                                "file_hash" : file_hash,
                                "initial_date" : initial_date,
                                "missing_date" : missing_date,
                                "mismatch_date" : mismatch_date,
                                "file_type" : file_type
                                }

        self.logger.info("Generated report from database results")
        return report

    def _check_existing_file_in_db(self, conn, file_path):
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM files WHERE file_path=?", (file_path,))
        return cursor.fetchone()

    def _clear_missing_date(self, conn, paths):
        cursor = conn.cursor()
        if len(paths) > 0: 
            sqlite_update_query = """UPDATE files SET missing_date=NULL WHERE file_path=?"""
            columnValues = [(x)for x in paths]
            cursor.executemany(sqlite_update_query, columnValues)
            conn.commit()

    def _update_missing_date(self, conn, paths):
        cursor = conn.cursor()
        if len(paths) > 0: 
            sqlite_update_query = """UPDATE files SET missing_date=? WHERE file_path=?"""
            columnValues = [(currentDateTime(), x)for x in paths]
            cursor.executemany(sqlite_update_query, columnValues)
            conn.commit()

    def _update_mismatch_date(self, conn, paths):
        cursor = conn.cursor()
        if len(paths) > 0: 
            sqlite_update_query = """UPDATE files SET mismatch_date=? WHERE file_path=?"""
            columnValues = [(currentDateTime(), x)for x in paths]
            cursor.executemany(sqlite_update_query, columnValues)
            conn.commit()
    
    def _clear_mismatch_date(self, conn, paths):
        cursor = conn.cursor()
        if len(paths) > 0: 
            sqlite_update_query = """UPDATE files SET mismatch_date=NULL WHERE file_path=?"""
            columnValues = [(x)for x in paths]
            cursor.executemany(sqlite_update_query, columnValues)
            conn.commit()

    def _insert_file_record(self, conn, paths):
        cursor = conn.cursor()
        if len(paths) > 0: 
            columnValues = []

            for path, hashValue in paths:
                # Get the initial date
                initial_date = currentDateTime()

                # Get File Type
                file_type = determine_file_type(path)

                columnValues.append((path,hashValue,initial_date,file_type))

            # Add the file's information to the database
            sqlite_update_query = """INSERT INTO files (file_path, file_hash, initial_date, file_type) VALUES (?, ?, ?, ?)"""
            cursor.executemany(sqlite_update_query, columnValues)
            conn.commit()

    def _delete_file_record(self, conn, paths):
        cursor = conn.cursor()
        if len(paths) > 0: 
            sqlite_update_query = """DELETE FROM files WHERE file_path=?"""
            columnValues = [(x)for x in paths]
            cursor.executemany(sqlite_update_query, columnValues)
            conn.commit()
        
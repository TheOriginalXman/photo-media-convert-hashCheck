import os
import sqlite3
import logging
from utility.dateTime import parse_date, get_current_datetime_string as currentDateTime
from utility.util import determine_file_type, get_file_hash as fileHash, get_configurations as getConfig 

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
        self.conn = None
        self.cursor = None
        self.connect_db(db_file_path)

    def _configure_logger(self):
        # dump all log levels to file
        self.logger.setLevel(logging.DEBUG)

        # create a file handler to log to a file
        file_handler = None

        # Check if we should append to the file or write a new file
        if self.config.get('singleFileLog', False):
            file_handler = logging.FileHandler(self.log_file, mode='a')
        else:
            file_handler = logging.FileHandler(self.log_file + currentDateTime(), mode='w')
            
        file_handler.setLevel(logging.DEBUG)

        # create a formatter for the logs
        formatter = logging.Formatter('%(asctime)s - %(process)d - %(thread)d - %(name)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # add the file handler to the logger
        self.logger.addHandler(file_handler)

    def create_db(self):
        """
        Creates the 'file_db.db' file in the root directory and creates the
        'files' table.
        """
        dbFilePath = os.path.join(self.db_folder_path, self.db_file_name)

        # check if the database file already exists
        if os.path.exists(dbFilePath):
            self.logger.debug(f"The database file already exists at {dbFilePath}")
            return
        
        self.logger.info(f"Creating a new database file at {dbFilePath}")
        
        # create the database file and table
        self.conn = sqlite3.connect(dbFilePath)
        self.cursor = self.conn.cursor()
        
        # Read the schema file
        try:
            with open("hashCheck/hash_db_schema.sql", "r") as f:
                schema = f.read()
        except Exception as e:
            self.logger.error(f"Error reading schema file: {e}")
            return
        
        # Execute the SQL commands in the schema file
        try:
            self.cursor.executescript(schema)
        except Exception as e:
            self.logger.error(f"Error executing schema script: {e}")
            return
        
        self.conn.commit()
        self.logger.info(f"Database file created successfully at {dbFilePath}")

    def scan_and_hash_files(self):
        """
        Loops through all root directories

        Scans all files in the root directory and nested subdirectories, 
        gets their hashes, and saves the information to db.
        """

        if self.root_directories:
            for root in self.root_directories:
                self.root_dir = root
                self._scan_and_hash_files()

    def _check_existing_file_in_db(self, file_path):
        self.cursor.execute("SELECT * FROM files WHERE file_path=?", (file_path,))
        return self.cursor.fetchone()

    def _clear_missing_date(self, file_path):
        self.cursor.execute("UPDATE files SET missing_date=NULL WHERE file_path=?", (file_path,))

    def _update_missing_date(self, file_path):
        self.cursor.execute("UPDATE files SET missing_date=? WHERE file_path=?", (currentDateTime(), file_path))

    def _update_mismatch_date(self, file_path):
        self.cursor.execute("UPDATE files SET mismatch_date=? WHERE file_path=?", (currentDateTime(), file_path))
    
    def _clear_mismatch_date(self, file_path):
        self.cursor.execute("UPDATE files SET mismatch_date=NULL WHERE file_path=?", (file_path,))

    def _insert_file_record(self, file_path, file_hash):
        # Get the initial date
        initial_date = currentDateTime()

        # Get File Type
        file_type = determine_file_type(file_path)

        # Add the file's information to the database
        self.cursor.execute("INSERT INTO files (file_path, file_hash, initial_date, file_type) VALUES (?, ?, ?, ?)", (file_path, file_hash, initial_date, file_type))
        self.conn.commit()

    def _delete_file_record(self, file_path):
        self.cursor.execute("DELETE FROM files WHERE file_path=?", (file_path,))
        self.conn.commit()

    def _scan_and_hash_files(self):
        """
        Scans all files in the root directory and nested subdirectories, 
        gets their hashes, and saves the information to db.
        """
        # Check if root directory is specified and exists
        if not self.root_dir or not os.path.exists(self.root_dir):
            self.logger.error('Root directory not found')
            print('Root directory not found')
            return

        # Connect to the database
        if not self._connect_to_db():
            self.logger.error('Failed to connect to the database')
            return

        self.logger.info(f'Scanning directory: {self.root_dir}')

        # Walk through all files and directories in the root directory
        for subdir, dirs, files in os.walk(self.root_dir):
            # Skip any directories in the skip list
            dirs = self._skip_directories(dirs)
            self.logger.debug(f"Directories to be skipped: {dirs}")

            for file in files:
                # Get the full file path
                file_path = subdir + os.sep + file

                # Log the current file being processed
                self.logger.info(f'Processing file: {file_path}')

                # Process the file
                self._process_file(file, file_path)

        # Commit changes and close the database connection
        self.conn.commit()
        self.logger.info('Committed changes to the database')
        self.conn.close()
        self.logger.info('Closed database connection')
    
    def _process_file(self,file,file_path):
        
        if self._skip_file(file, file_path):
            return
        # Check if the file is already in the database
        result = self._check_existing_file_in_db(file_path)
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
                    self.logger.debug(f"Clearing existing missing date for  {file_path}")
                    self._clear_missing_date(file_path)

                # Check if the hash has changed
                if file_hash != hash_value:
                    # update the mismatch date
                    self.logger.info(f'Hash mismatch for {file_path}')
                    self._update_mismatch_date(file_path)
                else:
                    if mismatch_date:
                        # Clear the mismatch date
                        self.logger.info(f'Clearing mismatch date for {file_path}')
                        self._clear_mismatch_date(file_path)
            else:
                self.logger.info(f'New file added {file_path}')
                self._insert_file_record(file_path, file_hash)
                
        else:
            # If File is missing
            if result:
                # Update the database with the missing date
                self.logger.info(f'File missing for {file_path}')
                self._update_missing_date(file_path)

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

    def _skip_directories(self, dirs):
        dirs[:] = [d for d in dirs if d not in self.exclusions.get("folderNames",[])]
        return dirs

    def _connect_to_db(self):
        self.connect_db()
        if not self.conn or not self.cursor:
            raise Exception("Database connection not established")
        return True

    def get_flagged_files(self, flag = None):
        """
        Returns the records that have either missing or mismatched dates based on the flag passed in. 
        flag = 'missing' or 'mismatch'
        """
        if not self._connect_to_db():
            self.logger.error("Failed to connect to database")
            return
        
        # Log the start of the function with an info log
        self.logger.info("Getting flagged files from database")

        # Get the records based on the flag
        if flag == "missing":
            self.cursor.execute("SELECT * FROM files WHERE missing_date IS NOT NULL")
        elif flag == "mismatch":
            self.cursor.execute("SELECT * FROM files WHERE mismatch_date IS NOT NULL")
        elif flag == None:
            self.cursor.execute("SELECT * FROM files WHERE mismatch_date IS NOT NULL OR missing_date IS NOT NULL")
        else:
            # Log an error if an invalid flag is passed in
            self.logger.error("Invalid flag. Please use 'missing' or 'mismatch'.")
            raise ValueError("Invalid flag. Please use 'missing' or 'mismatch'.")
            
        results = self.cursor.fetchall()
        
        # Log the number of results retrieved
        self.logger.debug(f"{len(results)} results retrieved")
        
        self.conn.close()

        report = self._get_report(results)
        return report

    def get_all_files(self):
        """
        Returns all files in the database
        """
        # Check if database is connected
        if not self._connect_to_db():
            self.logger.error("Failed to connect to database")
            return

        self.logger.debug("Fetching all files from database")
        self.cursor.execute("SELECT * FROM files")
        results = self.cursor.fetchall()
        self.conn.close()

        # Log the result of the query execution
        if results:
            self.logger.info(f"Found {len(results)} files")
        else:
            self.logger.warning(f"No files found")

        report = self._get_report(results)

        return report

    def get_files_by_type(self, file_type):
        """
        Returns the records that have the file type that is passed in
        """
        # Connect to the database and return if the connection was unsuccessful
        if not self._connect_to_db():
            self.logger.error("Failed to connect to database")
            return
        
        # Log the start of the function execution
        self.logger.debug(f"Getting files of type: {file_type}")
        
        # Execute the query to retrieve the file records
        self.cursor.execute("SELECT * FROM files WHERE file_type=?", (file_type,))
        results = self.cursor.fetchall()
        
        # Log the result of the query execution
        if results:
            self.logger.info(f"Found {len(results)} files of type {file_type}")
        else:
            self.logger.warning(f"No files found of type: {file_type}")
        
        # Call the function to get the report from the results
        report = self._get_report(results)
        
        # Return the report
        return report

    def get_files_by_initial_date(self,initial_date):
        """
        Retrieve records that match the initial date based on a date or datetime that is passed in as an argument.
        """
        # Log a debug message indicating the start of the function
        self.logger.debug("Getting files by initial date: %s", initial_date)

        # Check if the database connection is established
        if not self._connect_to_db():
            self.logger.error("Failed to connect to database")
            return

        # Log an info message indicating the parsing of the initial date
        self.logger.info("Parsing initial date: %s", initial_date)

        # Check if the passed-in argument is a string and parse it to a datetime if it is
        if isinstance(initial_date, str):
            initial_date = parse_date(initial_date)

        # Log a debug message indicating the execution of the SQL query
        self.logger.debug("Executing SQL query: SELECT * FROM files WHERE initial_date = %s", initial_date)

        # Execute the SQL query to retrieve the matching records
        self.cursor.execute("SELECT * FROM files WHERE initial_date = ?", (initial_date,))
        results = self.cursor.fetchall()
        self.conn.close()

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
        if not self._connect_to_db():
            self.logger.error("Failed to connect to the database")
            return

        try:
            self.cursor.execute("DELETE FROM files")
            self.conn.commit()
            self.logger.info("Successfully deleted all records from the database")
        except Exception as e:
            self.logger.error("An error occurred while deleting the records from the database: {}".format(e))
            return

        self.logger.info("Starting to re-scan all the files")
        self.scan_and_hash_files()
        self.logger.info("Finished re-scanning all the files")

    def reinitialize_specific(self, scan_list):
        """
        Re-scans specified folders or files and updates their hash and initial date values in the database.

        scan_list should include full paths to the folder or files
        """
        # Walk through all files and directories in the scan list
        for item in scan_list:
            if os.path.isdir(item):
                for subdir, dirs, files in os.walk(item):
                    # Skip any directories in the skip list
                    dirs = self._skip_directories(dirs)
                    self.logger.debug("Removed directories from iteration")

                    for file in files:
                        # Get the full file path
                        file_path = subdir + os.sep + file

                        # Skip files that are in the exclusion list
                        if self._skip_file(file,file_path):
                            self.logger.debug("File %s is in the exclusion list and will be skipped", file)
                            continue

                        # Re-initialize the record with the file path
                        self._reset_file_record(file_path)
                        self.logger.info("Record for file %s has been re-initialized", file)
            elif os.path.isfile(item):
                # Re-initialize the record with the file path
                self._reset_file_record(item)
                self.logger.info("Record for file %s has been re-initialized", item)
            else:
                self.logger.warning("%s is not a valid file or directory. Skipping...", item)

    def _reset_file_record(self,file_path):
        """
        Updates the hash, initial date and file type of a file, along with clearing the missing and mismatch dates
        """
        # Connect to the database if it's not connected
        if not self._connect_to_db():
            self.logger.error("Failed to connect to the database.")
            return

        # Get the file's hash
        file_hash = fileHash(file_path)
        self.logger.debug("Generated hash for file: {}".format(file_path))
        
        # Get the file's type
        file_type = determine_file_type(file_path)
        self.logger.debug("Determined file type for file: {}".format(file_path))

        # Delete the record from the database
        self._delete_file_record(file_path)
        self.logger.info("Deleted record for file: {}".format(file_path))

        # Insert a new record into the database
        self._insert_file_record(file_path, file_hash)
        self.logger.info("Inserted new record for file: {}".format(file_path))

    def connect_db(self, dbFilePath = None):
        '''
        Connects to the sqlite database. If it doesn't exist it creates a new db
        '''
        if not dbFilePath:
            dbFilePath = os.path.join(self.db_folder_path, self.db_file_name)

        # Check if the database file already exists
        if not os.path.exists(dbFilePath):
            self.logger.info("Creating database since it doesn't exist")
            self.create_db()
        try:
            self.conn = sqlite3.connect(dbFilePath)
            self.logger.info("Successfully connected to database")
        except Exception as e:
            self.logger.error("Error connecting to database: %s", e)
            raise Exception("Error connecting to database")

        if self.conn:
            self.cursor = self.conn.cursor()
            self.logger.debug("Cursor created successfully")
        else:
            self.logger.error("Database connection was not established")
            raise Exception("Database connection was not established")

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



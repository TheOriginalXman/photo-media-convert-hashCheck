import os
import sqlite3
import logging
from utility.dateTime import parse_date, get_current_datetime_string as currentDateTime
from utility.util import determine_file_type, get_file_hash as fileHash, get_configurations as getConfig 

class HashCheck:

    def __init__(self, config_path="../default_config.json", db_file_path=None):

        self.config = getConfig(config_path, 'hash')
        self.root_directories = self.config.get('rootFolderList', [])
        self.log_file = os.path.join(self.config.get('logFolderParentFolderPath', './'), self.config.get('logFileName', 'hash.log'))
        self.exclusions = self.config.get('exclusions',{})
        self.db_file_name = self.config.get('dbFile','hash.db')
        self.db_folder_path = self.config.get('dbFileParentFolderPath','./')
        self.root_dir = None
        self.conn = None
        self.cursor = None
        self.connect_db(db_file_path)
        self.logger = self._create_logger()

    def _create_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # create a file handler
        
        handler = logging.FileHandler(self.log_file)
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        return logger

    def create_db(self):
        """
        Creates the 'file_db.db' file in the root directory and creates the
        'files' table.
        """
        dbFilePath = os.path.join(self.db_folder_path, self.db_file_name)

        # check if the database file already exists
        if os.path.exists(dbFilePath):
            return
        # create the database file and table
        self.conn = sqlite3.connect(dbFilePath)
        self.cursor = self.conn.cursor()
        # Read the schema file
        with open("hashCheck/hash_db_schema.sql", "r") as f:
            schema = f.read()

        # Execute the SQL commands in the schema file
        self.cursor.executescript(schema)

        self.conn.commit()

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


    def _scan_and_hash_files(self):
        """
        Scans all files in the root directory and nested subdirectories, 
        gets their hashes, and saves the information to db.
        """
        if not self.root_dir or not os.path.exists(self.root_dir):
            print('Root directory not found')
            return

        self.connect_db()

        if not self.conn or not self.cursor:
            raise Exception("Database connection not established")

        # Walk through all files and directories in the root directory
        for subdir, dirs, files in os.walk(self.root_dir):
            # Skip any directories in the skip list
            dirs[:] = [d for d in dirs if d not in self.exclusions.get("folderNames",[])]
            for file in files:
                # Skip files in the skip list
                if any(file.endswith(ext) for ext in self.exclusions.get("extensions",[])) or file in self.exclusions.get("fileNames", []):
                    continue

                # Get the full file path
                file_path = subdir + os.sep + file

                #Skip files in exclusion paths
                if self.exclusions.get('paths', None):
                    skipFlag = False
                    for path in self.exclusions.get('paths', []):
                        if os.path.commonpath([path, file_path]) == path:
                            skipFlag = True
                            break
                    if skipFlag:
                        continue

                # Check if the file is already in the database
                self.cursor.execute("SELECT * FROM files WHERE file_path=?", (file_path,))
                result = self.cursor.fetchone()

                # Check if the file still exists
                if os.path.exists(file_path):
                    # Get the file's hash
                    file_hash = fileHash(file_path)

                    if result: 
                        # Clear missing date if exists
                        if result[3]:
                            self.cursor.execute("UPDATE files SET missing_date=NULL WHERE file_path=?", (file_path,))

                        # Check if the hash has changed
                        if file_hash != result[1]:
                            # update the mismatch date
                            self.cursor.execute("UPDATE files SET mismatch_date=? WHERE file_path=?", (currentDateTime(), file_path))
                            print(f'Hash mismatch for {file_path}')

                        else:
                            if result[4]:
                                # Clear the mismatch date
                                self.cursor.execute("UPDATE files SET mismatch_date=NULL WHERE file_path=?", (file_path,))
                    else:
                        # Get the initial date
                        initial_date = currentDateTime()

                        # Get File Type
                        file_type = determine_file_type(file_path)

                        # Add the file's information to the database
                        self.cursor.execute("INSERT INTO files (file_path, file_hash, initial_date, file_type) VALUES (?, ?, ?, ?)", (file_path, file_hash, initial_date, file_type))
                        print(f'New file added {file_path}')
                else:
                    # If File is missing
                    if result:
                        # Update the database with the missing date
                        self.cursor.execute("UPDATE files SET missing_date=? WHERE file_path=?", (currentDateTime(), file_path))
                        print(f'File missing for {file_path}')
        self.conn.commit()
        self.conn.close()


    def get_flagged_files(self, flag = None):
        """
        Returns the records that have either missing or mismatched dates based on the flag passed in. 
        flag = 'missing' or 'mismatch'
        """
        self.connect_db()

        if not self.conn or not self.cursor:
            raise Exception("Database connection not established")

        if flag == "missing":
            self.cursor.execute("SELECT * FROM files WHERE missing_date IS NOT NULL")
        elif flag == "mismatch":
            self.cursor.execute("SELECT * FROM files WHERE mismatch_date IS NOT NULL")
        elif flag == None:
            self.cursor.execute("SELECT * FROM files WHERE mismatch_date IS NOT NULL OR missing_date IS NOT NULL")
        else:
            raise ValueError("Invalid flag. Please use 'missing' or 'mismatch'.")
        results = self.cursor.fetchall()
        self.conn.close()

        report = self._get_report(results)
        return report

    def get_all_files(self):
        """
        Returns all files in the database
        """
        self.connect_db()

        if not self.conn or not self.cursor:
            raise Exception("Database connection not established")

        self.cursor.execute("SELECT * FROM files")
        results = self.cursor.fetchall()
        self.conn.close()

        report = self._get_report(results)
        return report

    def get_files_by_type(self, type):
        """
        Returns the records that have the file type that is passed in
        """
        self.connect_db()

        if not self.conn or not self.cursor:
            raise Exception("Database connection not established")

        self.cursor.execute("SELECT * FROM files WHERE file_type=?", (type,))
        results = self.cursor.fetchall()

        report = self._get_report(results)
        return report

    def get_files_by_initial_date(self,initial_date):
        """
        Retrieve records that match the initial date based on a date or datetime that is passed in as an argument.
        """
        self.connect_db()

        if not self.conn or not self.cursor:
            raise Exception("Database connection not established")

        if isinstance(initial_date, str):
            initial_date = parse_date(initial_date)
        self.cursor.execute("SELECT * FROM files WHERE initial_date = ?", (initial_date,))
        results = self.cursor.fetchall()
        self.conn.close()

        report = self._get_report(results)
        return report

    def reinitialize_db(self):
        """
        Resets all the fields of the db and re-scan all files
        """
        if not self.conn or not self.cursor:
            raise Exception("Database connection not established")

        self.cursor.execute("DELETE FROM files")
        self.conn.commit()
        self.scan_and_hash_files()

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
                    dirs[:] = [d for d in dirs if d not in self.exclusions.get("folderNames",[])]
                    for file in files:
                        # Skip files in the skip list
                        if any(file.endswith(ext) for ext in self.exclusions.get("extensions",[])) or file in self.exclusions.get("fileNames", []):
                            continue
                        # Get the full file path
                        file_path = subdir + os.sep + file
                        # Update the hash and initial date
                        self._update_hash_initial_date(file_path)
            elif os.path.isfile(item):
                # Update the hash and initial date
                self._update_hash_initial_date(item)

    def _update_hash_initial_date(self,file_path):
        """
        Updates the hash and initial date of a file
        """
        self.connect_db()
        
        if not self.conn or not self.cursor:
            raise Exception("Database connection not established")

        # Get the file's hash
        file_hash = fileHash(file_path)
        # Get the file's type
        file_type = determine_file_type(file_path)
        # Check if the file is already in the database
        self.cursor.execute("SELECT * FROM files WHERE file_path=?", (file_path,))
        result = self.cursor.fetchone()
        if result:
            # Update the file in the database
            self.cursor.execute("UPDATE files SET file_hash=?, initial_date=?, missing_date=NULL, mismatch_date=NULL, file_type=? WHERE file_path=?", (file_hash, currentDateTime(), file_type, file_path))
            self.conn.commit()

    def connect_db(self, dbFilePath = None):
        if not dbFilePath:
            dbFilePath = os.path.join(self.db_folder_path, self.db_file_name)
        # check if the database file already exists
        if not os.path.exists(dbFilePath):
            self.create_db()
        self.conn = sqlite3.connect(dbFilePath)

        if self.conn:
            self.cursor = self.conn.cursor()
        else:
            raise Exception('Database connection was not established')

    def _get_report(self, db_results):
        report = {}

        for row in db_results:
            file_path, file_hash, initial_date, missing_date, mismatch_date, file_type = row

            report[file_path] = {
                                "file_path" : file_path, 
                                "file_hash" : file_hash,
                                "initial_date" : initial_date,
                                "missing_date" : missing_date,
                                "mismatch_date" : mismatch_date,
                                "file_type" : file_type
                                }
        return report



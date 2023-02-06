            file_handler = logging.FileHandler(currentDateTime() + ' ' + self.log_file , mode='w')
        # create a console handler to log to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        file_formatter = logging.Formatter('%(asctime)s - %(process)d - %(thread)d - %(name)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

            file_handler = logging.FileHandler(currentDateTime() + ' ' + self.log_file , mode='w')
        # create a console handler to log to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        file_formatter = logging.Formatter('%(asctime)s - %(process)d - %(thread)d - %(name)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

            file_handler = logging.FileHandler(currentDateTime() + ' ' + self.log_file , mode='w')
            file_handler = logging.FileHandler(currentDateTime() + ' ' + self.log_file , mode='w')
        # create a console handler to log to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        file_formatter = logging.Formatter('%(asctime)s - %(process)d - %(thread)d - %(name)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(asctime)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)

            
            dirnames = self._remove_excluded_folders_from_traverse_path(dirnames)

            dirnames = self._remove_excluded_files_from_traverse_path(filenames)
    def _remove_excluded_files_from_traverse_path(self, fileNames):
        # Remove all the exclusion folders from the directory walk
        exclusion_files = self.exclusions.get('fileNames',[])
        for file in exclusion_files:
            if file in fileNames:
                fileNames.remove(file)
        
        self.logger.debug('Removed Excluded Files')

        return fileNames
    
    def _remove_excluded_folders_from_traverse_path(self, folderNames):
        exclusion_folders = self.exclusions.get('folderNames',[])
        exclusion_folders.append(self.converted_folder_name)
        for folder in exclusion_folders:
            if folder in folderNames:
                folderNames.remove(folder)
        
        self.logger.debug('Removed Excluded Folders')

        return folderNames

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


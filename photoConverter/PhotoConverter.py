import os
import datetime
import subprocess
import platform
import logging
from PIL import Image
from utility.util import get_configurations as getConfig 
from utility.dateTime import get_current_datetime_string as currentDateTime


class PhotoConverter:
    def __init__(self, config_path="../default_config.json", root_path=None):

        # Load configuration from the given path
        self.config = getConfig(config_path, 'photo')

        # Get the log file path and name from the config file and setup logger
        self.log_file = os.path.join(self.config.get('logFolderParentFolderPath', None), self.config.get('logFileName', None))

        self.logger = logging.getLogger(__name__)
        self._configure_logger()
        self.logger.debug('Log File Path: {0}'.format(self.log_file))

        # Get the root directories from the config file, if not specified, use the root_path
        self.root_directories = self.config.get('rootFolderList', [root_path])
        self.logger.debug('Root Directories: {0}'.format(self.root_directories))

        # Get the converted folder path and name from the config file
        self.converted_folder_path = os.path.join(self.config.get('convertedFolderParentFolderPath', ''), self.config.get('convertedFolderName', None))
        self.logger.debug('Converted Folder Path: {0}'.format(self.converted_folder_path))
        self.converted_folder_name = self.config.get('convertedFolderName', 'conversion')

        # Get the exclusions from the config file
        self.exclusions = self.config.get('exclusions',{})
        self.logger.debug('Exclusions: {0}'.format(self.exclusions))

        # Get the input formats from the config file
        self.input_ext = self.config.get('queryExtensions',None)
        self.logger.debug('Input Formats: {0}'.format(self.input_ext))

        # Get the output format from the config file
        self.output_ext = self.config.get('outputExtension',None)
        self.logger.debug('Output Format: {0}'.format(self.output_ext))

        # Set the root path and root directory
        self.root_path = root_path
        self.root_dir = None

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
        
    def install_dependencies(self):
        if platform.system() == "Darwin":
            try:
                output = subprocess.run(['which', 'sips'], capture_output=True, check=True)
                if not output.stdout:
                    subprocess.run(['brew', 'install', 'sips'], check=True)
                    print("sips is installed")
            except subprocess.CalledProcessError as e:
                print(f"Error installing sips: {e}")
        elif platform.system() == "Linux":
            subprocess.run(["apt-get", "install", "libheif-examples"])
        else:
            raise Exception("libheif installation not supported on this platform.")

    def _convert(self, input_formats = None, output_format = None):
        # Check if there are no input formats defined
        if not input_formats or len(input_formats) == 0:
            self.logger.error('No input formats defined')
            return
        # Check if there is no output format defined
        elif output_format == None:
            self.logger.error('No output format defined')
            return
        # Check if the root directory does not exist
        elif not self.root_dir or not os.path.exists(self.root_dir):
            self.logger.error('Root directory not found')
            return
        
        # Make sure the output format is in lowercase
        output_format = output_format.lower()
        self.logger.debug('Output format: %s' % output_format)
        # Make sure all input formats are in lowercase
        input_formats = [f.lower() for f in input_formats]
        self.logger.debug('Input formats: %s' % input_formats)

        # Start the directory walk
        for dirpath, dirnames, filenames in os.walk(self.root_dir):
            # Remove all the exclusion folders and files from the directory walk
            exclusion_folders = self.exclusions.get('folderNames',[])
            exclusion_folders.append(self.converted_folder_name)
            for folder in exclusion_folders:
                if folder in dirnames:
                    dirnames.remove(folder)
            self.logger.debug('Removed Excluded Folders')
            exclusion_files = self.exclusions.get('fileNames',[])
            for file in exclusion_files:
                if file in filenames:
                    filenames.remove(file)
            
            self.logger.debug('Removed Excluded Files')

            self._convert_process_directories(dirpath, filenames, input_formats, output_format)
            
    def _convert_process_directories(self,dirpath,filenames, input_formats, output_format):
        self.logger.debug('Processing Directories')
        # Get the converted folder path
        output_folder = os.path.join(dirpath, self.converted_folder_name)
        if self.config.get('convertedFolderParentFolderPath',None):
            output_folder = self.converted_folder_path

        self.logger.debug('Output Folder Path: {0}'.format(output_folder))

        # Loop through all the files in the directory
        for file in filenames:
            self._convert_process_file(dirpath, file, input_formats, output_format, output_folder)

    def _convert_process_file(self, dirpath, file, input_formats, output_format, output_folder):
        # Get the full path of the input file
        input_file = os.path.join(dirpath, file)
        self.logger.debug('Input File Path: {0}'.format(input_file))

        # Get file extension
        extension = os.path.splitext(file)[1].lower()
        self.logger.debug('Extension: {0}'.format(extension))

        if self._is_file_excluded(extension,input_file, input_formats):
            self.logger.debug('File excluded: {0}'.format(input_file))
            return

        # Create the converted photo folder if it doesn't exist
        if not os.path.exists(output_folder):
            os.mkdir(output_folder)
            self.logger.info('Photo conversion folder created: {0}'.format(output_folder))
            print('Photo conversion folder created')
        
        # Get the output file name
        output_file = os.path.join(output_folder, file.replace(extension, '.' + output_format).replace(extension.upper(), '.' + output_format))
        self.logger.debug('Output file: {0}'.format(output_file))
        # Check if the output file already exists
        if os.path.exists(output_file):
            self.logger.debug('Output file already exists: {0}'.format(output_file))
            return
        self.logger.info('Converting Image')
        # Convert the file based on the file extension
        if(extension == '.heic'):
            if platform.system() == "Darwin":
                self.convert_heic_mac(input_file, output_file,output_format)
            elif platform.system() == "Linux":
                self.convert_heic_linux(input_file, output_file,output_format)
        else:
            self.convert_img(input_file, output_file, output_format)
        
        if(os.path.exists(output_file)):
            self.logger.info('Successfully Converted Photo')
        else:
            self.logger.info('Failed to convert file: {}'.format(input_file))

    def _is_file_excluded(self, extension, input_file, input_formats):
        # Check if the file needs to be excluded based on file extension
        if extension in self.exclusions.get('extensions', []):
            self.logger.debug('Excluded based on file extension')
            return True

        # Check if the file needs to be excluded based on exclusion paths provided
        if self.exclusions.get('paths', None):
            skipFlag = False
            for path in self.exclusions.get('paths', []):
                if os.path.commonpath([path, input_file]) == path:
                    skipFlag = True
                    break
            if skipFlag:
                self.logger.debug('Excluded. File path with exclusion path')
                return True
        
        # Check if the file extension is in the list of input formats
        if extension[1:] not in input_formats:
            self.logger.debug('Skipping extension is not in input formats list')
            return True

    def convert_heic_linux(self, input_file, output_file, output_format):
        # Convert the HEIC file to JPG
        self.logger.debug('Converting HEIC on Linux')
        try:
            if(output_format in ['png', 'jpg','jpeg']):
                subprocess.run(['heif-convert', '-q', '100', input_file, output_file])
            else:
                self.logger.warning('conversion not supported')
        except subprocess.CalledProcessError as e:
            self.logger.error(f'Error converting {input_file} to {output_file}. Error: {e}\n')

    def convert_heic_mac(self, input_file, output_file, output_format):
        # Convert the HEIC file to JPG
        self.logger.debug('Converting HEIC on Mac')
        try:
            subprocess.run(['sips', '-s', 'format', output_format, input_file, '--out', output_file])
        except subprocess.CalledProcessError as e:
            self.logger.error(f'Error converting {input_file} to {output_file}. Error: {e}\n')
    
    def convert_img(self, input_file, output_file, output_format):
        # Convert the input file to the output format
        try:
            with Image.open(input_file) as img:
                img.save(output_file, format=output_format)
        except Exception as e:
            self.logger.error(f'Error converting {input_file} to {output_file}. Error: {e}\n')

    def convert(self, input_formats = None, output_format = None):

        # if input_formats are not specified, use the input_ext attribute of the class
        if not input_formats:
            input_formats = self.input_ext
        # if output_format is not specified, use the output_ext attribute of the class
        if not output_format:
            output_format = self.output_ext
            
        # Check if root directories have been specified in the config
        if self.root_directories:
            # If root directories have been specified, loop through each root directory
            if len(self.root_directories) > 0:
                for root in self.root_directories:
                    self.root_dir = root
                    self._convert(input_formats, output_format)
            else:
                # If no root directories have been specified, log the error
                self.logger.error('No root directories specified in config')
        # If root path has been specified, use that
        elif self.root_path:
            self.root_dir = self.root_path
            self._convert(input_formats, output_format)

        
    
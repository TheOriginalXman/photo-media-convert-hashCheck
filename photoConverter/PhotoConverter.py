import os
import datetime
import subprocess
import platform
import logging
from PIL import Image
from utility.util import get_configurations as getConfig 

class PhotoConverter:
    def __init__(self, config_path="../default_config.json", root_path=None):
        # self.install_dependencies()
        self.config = getConfig(config_path, 'photo')
        self.root_directories = self.config.get('rootFolderList', [root_path])
        self.log_file = os.path.join(self.config.get('logFolderParentFolderPath', None), self.config.get('logFileName', None))
        self.converted_folder_path = os.path.join(self.config.get('convertedFolderParentFolderPath', ''), self.config.get('convertedFolderName', None))
        self.converted_folder_name = self.config.get('convertedFolderName', 'conversion')
        self.exclusions = self.config.get('exclusions',{})
        self.input_ext = self.config.get('queryExtensions',None)
        self.output_ext = self.config.get('outputExtension',None)
        self.root_path = root_path
        self.root_dir = None
        self.logger = logging.getLogger(__name__)
        self.configure_logger()
    
    def configure_logger(self):
        self.logger.setLevel(logging.DEBUG)
        # create a file handler to log to a file
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(logging.DEBUG)

        # create a formatter for the logs
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
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
            #Remove all the exclusion folders and files from the directory walk
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

            # Get the converted folder path
            jpg_folder = os.path.join(dirpath, self.converted_folder_name)
            if self.config.get('convertedFolderParentFolderPath',None):
                jpg_folder = self.converted_folder_path

            self.logger.debug('Output Folder Path: {0}'.format(jpg_folder))
            
            # Loop through all the files in the directory
            for file in filenames:
                # Get the full path of the input file
                input_file = os.path.join(dirpath, file)
                self.logger.debug('Input File Path: {0}'.format(input_file))

                # Get file extension
                extension = os.path.splitext(file)[1].lower()
                self.logger.debug('Extension: {0}'.format(extension))

                # Check if the file needs to be excluded based on file extension
                if extension in self.exclusions.get('extensions', []):
                    self.logger.debug('Excluded based on file extension')
                    continue

                # Check if the file needs to be excluded based on exclusion paths provided
                if self.exclusions.get('paths', None):
                    skipFlag = False
                    for path in self.exclusions.get('paths', []):
                        if os.path.commonpath([path, input_file]) == path:
                            skipFlag = True
                            break
                    if skipFlag:
                        print('Excluded file being in an excluded path')
                        continue

                # Check if the file extension is in the list of input formats
                if extension[1:] in input_formats:
                    print('Extension in input formats list')
                    #Create the converted photo folder if it doesn't exist'
                    if not os.path.exists(jpg_folder):
                        os.mkdir(jpg_folder)
                        print('Photo conversion folder created')
                    
                    # Get the output file name
                    output_file = os.path.join(jpg_folder, file.replace(extension, '.' + output_format).replace(extension.upper(), '.' + output_format))
                    print('output_file: {}'.format(output_file))
                    # Check if the output file already exists
                    if os.path.exists(output_file):
                        print('Output file already exists')
                        continue

                    if(extension == '.heic'):
                        # self.convert_heic_linux(input_file, output_file,output_format)
                        if platform.system() == "Darwin":
                            self.convert_heic_mac(input_file, output_file,output_format)
                        elif platform.system() == "Linux":
                            self.convert_heic_linux(input_file, output_file,output_format)
                    else:
                        self.convert_img(input_file, output_file, output_format)
                
                else:
                    print('Skipping extension is not in input formats list')
                    continue
    
    def convert_heic_linux(self, input_file, output_file, output_format):
        # Convert the HEIC file to JPG
        print('Converting HEIC on Linux')
        try:
            if(output_format in ['png', 'jpg','jpeg']):
                subprocess.run(['heif-convert', '-q', '100', input_file, output_file])
            else:
                print('conversion not supported')
        except subprocess.CalledProcessError as e:
            print(f'[{datetime.datetime.now()}] Error converting {input_file} to {output_file}. Error: {e}\n')

    def convert_heic_mac(self, input_file, output_file, output_format):
        # Convert the HEIC file to JPG
        print('Converting HEIC on Mac')
        try:
            subprocess.run(['sips', '-s', 'format', output_format, input_file, '--out', output_file])
        except subprocess.CalledProcessError as e:
            print(f'[{datetime.datetime.now()}] Error converting {input_file} to {output_file}. Error: {e}\n')
    
    def convert_img(self, input_file, output_file, output_format):
        # Convert the input file to the output format
        try:
            with Image.open(input_file) as img:
                img.save(output_file, format=output_format)
        except Exception as e:
            print(f'[{datetime.datetime.now()}] Error converting {input_file} to {output_file}. Error: {e}\n')

    def convert(self, input_formats = None, output_format = None):

        if not input_formats:
            input_formats = self.input_ext
        if not output_format:
            output_format = self.output_ext

        if self.root_directories:
            if len(self.root_directories) > 0:
                for root in self.root_directories:
                    self.root_dir = root
                    self._convert(input_formats, output_format)
            else:
                print('No root directories specified in config')
        elif self.root_path:
            self.root_dir = self.root_path
            self._convert(input_formats, output_format)
        
    
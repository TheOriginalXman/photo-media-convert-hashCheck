import importlib
import os
import datetime
import subprocess
import platform
import mimetypes
import logging
from utility.util import get_configurations as getConfig 
from utility.dateTime import get_current_datetime_string as currentDateTime


class VideoConverter:
    def __init__(self, config_path="../default_config.json", root_path=None):
        
        # Load configuration from the given path
        self.config = getConfig(config_path, 'video')
        # Get the log file path and name from the config file and setup logger
        self.log_file = os.path.join(self.config.get('logFolderParentFolderPath', None), self.config.get('logFileName', None))

        self.logger = logging.getLogger(__name__)
        self._configure_logger()
        self.logger.debug('Log File Path: {0}'.format(self.log_file))

        self.check_requirements()
        self.root_directories = self.config.get('rootFolderList', [root_path])
        self.converted_folder_path = os.path.join(self.config.get('convertedFolderParentFolderPath', ''), self.config.get('convertedFolderName', None))
        self.converted_folder_name = self.config.get('convertedFolderName', 'conversion')
        self.exclusions = self.config.get('exclusions',{})
        self.root_path = root_path
        self.root_dir = None
        self.output_ext = self.config.get('outputExtension',None)
        
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
            file_handler = logging.FileHandler(self.log_file + currentDateTime(), mode='w')

        file_handler.setLevel(numeric_log_level)

        # create a formatter for the logs
        formatter = logging.Formatter('%(asctime)s - %(process)d - %(thread)d - %(name)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)

        # add the file handler to the logger
        self.logger.addHandler(file_handler)

    def check_requirements(self):
        try:
            # Check if ffmpeg is installed
            subprocess.run(['ffmpeg', '-version'], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            self.logger.info('ffmpeg is installed')
        except Exception as e:
            # Log a warning if ffmpeg is not installed
            self.logger.warning('ffmpeg not installed: install using function install_requirements or via cli for your platform')
            print('ffmpeg not installed: install using function install_requirements or via cli for your platform')

    def install_requirements(self):
        try:
            # Try to run ffmpeg
            output = subprocess.run(["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
            self.logger.debug(f"Output from running ffmpeg: {output}")
        except subprocess.CalledProcessError:
            # Log a warning if ffmpeg call fails
            self.logger.warning("ffmpeg not found. Installing ffmpeg...")
            print("ffmpeg not found. Installing ffmpeg...")
            self.install_ffmpeg()
        except FileNotFoundError:
            # Log a warning if ffmpeg call fails
            self.logger.warning("ffmpeg not found. Installing ffmpeg...")
            print("ffmpeg not found. Installing ffmpeg...")
            self.install_ffmpeg()

    def install_ffmpeg(self):
        if platform.system() == "Darwin":
            # Log the installation command for Mac
            self.logger.info("Installing ffmpeg on Mac")
            subprocess.run(["brew", "install", "ffmpeg"])
        elif platform.system() == "Linux":
            # Log the installation command for Linux
            self.logger.info("Installing ffmpeg on Linux")
            subprocess.run(["apt-get", "install", "ffmpeg"])
        else:
            # Log an error if the platform is not supported
            self.logger.error("ffmpeg installation not supported on this platform.")
            raise Exception("ffmpeg installation not supported on this platform.")

    def is_video_file(self,file_path):
        # Check if the file is a video file
        mime = mimetypes.guess_type(file_path)[0]
        return mime and mime.startswith('video/')

    def convert(self, output_format = None):
        # Check if output format is provided as parameter
        if not output_format:
            output_format = self.output_ext
            self.logger.info(f'No output format defined, defaulting to {output_format}')

        # Use root directories defined in config. If not defined used directory that initialized the instance
        if self.root_directories:
            for root in self.root_directories:
                self.root_dir = root
                self.logger.debug(f'Converting files in root directory: {root}')
                self._convert(output_format)
        elif self.root_path:
            self.root_dir = self.root_path
            self.logger.debug(f'Converting files in root path: {self.root_path}')
            self._convert(output_format)

    def _convert(self, output_format = None):
        if not self.root_dir or not os.path.exists(self.root_dir):
            # Log a warning message
            self.logger.warning('Root directory not found')
            return

        if not output_format:
            # Log a warning message
            self.logger.warning('No output format defined')
            return

        # Iterate through all files in the root directory and its subdirectories
        for subdir, dirs, files in os.walk(self.root_dir):

            exclusion_folders = self.exclusions.get('folderNames',[])
            exclusion_folders.append(self.converted_folder_name)
            for folder in exclusion_folders:
                if folder in dirs:
                    dirs.remove(folder)

            exclusion_files = self.exclusions.get('fileNames',[])
            for file in exclusion_files:
                if file in files:
                    files.remove(file)

            self.logger.debug('All excluded folders and files removed from iteration')

            for file in files:
                # Get the file path
                file_path = os.path.join(subdir, file)
                self.logger.debug('file_path: %s' % file_path)
                # Create the output file path
                if self.config.get('convertedFolderParentFolderPath',None):
                    target_file_path = os.path.join(self.converted_folder_path, file) + '.' + output_format
                else:
                    target_file_path = os.path.join(subdir,self.converted_folder_name, file) + '.' + output_format
                self.logger.debug('target_file_path: {}'.format(target_file_path))
                # Get file extension
                extension = os.path.splitext(file)[1].lower()
                self.logger.debug('file extension: {}'.format(extension))

                # Check if the file is a video file and if it is already in the specified format
                if not self.is_video_file(file_path):
                    # Log a debug message
                    self.logger.debug('Skipping file: {} as it is not a video file'.format(file_path))
                    continue

                if extension[1:] in [f.lower() for f in self.exclusions.get("extensions", [])]:
                    # Log a debug message
                    self.logger.debug('Skipping file: {} as it has an excluded extension'.format(file_path))
                    continue

                # Check if the file needs to be excluded based on exclusion paths provided
                if self.exclusions.get('paths', None):
                    skipFlag = False
                    for path in self.exclusions.get('paths', []):
                        if os.path.commonpath([path, file_path]) == path:
                            skipFlag = True
                            break
                    if skipFlag:
                        # Log a debug message
                        self.logger.debug('Skipping file: {} as it is in an excluded path'.format(file_path))
                        continue

                if os.path.exists(target_file_path):
                    # Log a debug message
                    self.logger.debug('Skipping file: {} as it is already converted'.format(file_path))
                    continue

                # Create the subdirectory for the converted files if it doesn't already exist
                if self.config.get('convertedFolderParentFolderPath',None) and not os.path.exists(self.converted_folder_path):
                    os.mkdir(self.converted_folder_path)
                    self.logger.debug('Converted folder created at %s', self.converted_folder_path)
                elif not os.path.exists(os.path.join(subdir, self.converted_folder_name)):
                    os.mkdir(os.path.join(subdir, self.converted_folder_name))
                    self.logger.debug("Converted folder created at %s" % self.converted_folder_name)

                # Use ffmpeg to convert the video file to the specified format
                try:
                    self.logger.info("Converting video file {}".format(file_path))
                    subprocess.run(['ffmpeg', '-i', file_path, target_file_path], check=True)
                except subprocess.CalledProcessError as e:
                    self.logger.error('Error while converting Error: {e}\n')

                if(os.path.exists(target_file_path)):
                    self.logger.info('Successfully Converted Video')
                else:
                    self.logger.info('Failed to convert file: {}'.format(file_path))
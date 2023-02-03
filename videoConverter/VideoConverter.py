import importlib
import os
import datetime
import subprocess
import platform
import mimetypes
import logging
from utility.util import get_configurations as getConfig 


class VideoConverter:
    def __init__(self, config_path="../default_config.json", root_path=None):
        if not self.check_requirements():
            self.install_requirements()

        self.config = getConfig(config_path, 'video')
        self.root_directories = self.config.get('rootFolderList', [root_path])
        self.log_file = os.path.join(self.config.get('logFolderParentFolderPath', None), self.config.get('logFileName', None))
        self.converted_folder_path = os.path.join(self.config.get('convertedFolderParentFolderPath', ''), self.config.get('convertedFolderName', None))
        self.converted_folder_name = self.config.get('convertedFolderName', 'conversion')
        self.exclusions = self.config.get('exclusions',{})
        self.root_path = root_path
        self.root_dir = None
        self.output_ext = self.config.get('outputExtension',None)
        
        formatter = logging.Formatter('%(asctime)s - %(process)d - %(thread)d - %(name)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s')
    def check_requirements(self):
        try:
            subprocess.run(['ffmpeg', '-version'], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True
        except Exception as e:
            return False

    def install_requirements(self):
        try:
            output = subprocess.run(["ffmpeg", "-version"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, check=True)
        except subprocess.CalledProcessError:
            print("ffmpeg not found. Installing ffmpeg...")
            self.install_ffmpeg()
        except FileNotFoundError:
            print("ffmpeg not found. Installing ffmpeg...")
            self.install_ffmpeg()

    def install_ffmpeg(self):
        if platform.system() == "Darwin":
            subprocess.run(["brew", "install", "ffmpeg"])
        elif platform.system() == "Linux":
            subprocess.run(["apt-get", "install", "ffmpeg"])
        else:
            raise Exception("ffmpeg installation not supported on this platform.")

    def is_video_file(self,file_path):
        mime = mimetypes.guess_type(file_path)[0]
        return mime and mime.startswith('video/')

    def convert(self, output_format = None):

        if not output_format:
            output_format = self.output_ext

        if self.root_directories:
            for root in self.root_directories:
                self.root_dir = root
                self._convert(output_format)
        elif self.root_path:
            self.root_dir = self.root_path
            self._convert(output_format)

    def _convert(self, output_format = None):
        if not self.root_dir or not os.path.exists(self.root_dir):
            print('Root directory not found')
            return
        if not output_format:
            print('No output format defined')
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

            for file in files:
                # Get the file path
                file_path = os.path.join(subdir, file)
                # Create the output file path
                if self.config.get('convertedFolderParentFolderPath',None):
                    target_file_path = os.path.join(self.converted_folder_path, file) + '.' + output_format
                else:
                    target_file_path = os.path.join(subdir,self.converted_folder_name, file) + '.' + output_format

                # Get file extension
                extension = os.path.splitext(file)[1].lower()

                # Check if the file is a video file and if it is already in the specified format
                if not self.is_video_file(file_path):
                    continue

                if extension[1:] in [f.lower() for f in self.exclusions.get("extensions", [])]:
                    continue

                # Check if the file needs to be excluded based on exclusion paths provided
                if self.exclusions.get('paths', None):
                    skipFlag = False
                    for path in self.exclusions.get('paths', []):
                        if os.path.commonpath([path, file_path]) == path:
                            skipFlag = True
                            break
                    if skipFlag:
                        continue

                if os.path.exists(target_file_path):
                    continue

                # Create the subdirectory for the converted files if it doesn't already exist
                if self.config.get('convertedFolderParentFolderPath',None) and not os.path.exists(self.converted_folder_path):
                    os.mkdir(self.converted_folder_path)
                elif not os.path.exists(os.path.join(subdir, self.converted_folder_name)):
                    os.mkdir(os.path.join(subdir, self.converted_folder_name))

                # Use ffmpeg to convert the video file to the specified format
                try:
                    subprocess.run(['ffmpeg', '-i', file_path, target_file_path], check=True)
                except subprocess.CalledProcessError as e:
                    # self.log_file.write(f'[{datetime.datetime.now()}] Error converting {file_path}: {e}\n')
                    print('Error while converting Error: {e}\n')

        # Close the log file
        # self.log_file.close()


from hashCheck.HashCheck import HashCheck
from videoConverter.VideoConverter import VideoConverter
from photoConverter.PhotoConverter import PhotoConverter
import os



path = os.path.realpath('config.json')
    
imgConverter = PhotoConverter(path)
imgConverter.convert()
videoConverter = VideoConverter(path)
videoConverter.convert()
hashCheck = HashCheck(path)
hashCheck.scan_and_hash_files()
# flagged_files = hashCheck.get_flagged_files()
# hashCheck.reinitialize_specific(list(flagged_files.keys()))


import sys
from photoConverter.PhotoConverter import PhotoConverter
import os



# path = os.path.realpath('config.json')
    
imgConverter = PhotoConverter()
imgConverter.convert()
# imgConverter.remove_all_converted_files()
imgConverter.move_missing_files_from_converted_to_actual_directory()
# videoConverter = VideoConverter(path)
# videoConverter.convert()
# hashCheck = HashCheck(path)
# hashCheck.scan_and_hash_files()
# flagged_files = hashCheck.get_flagged_files()
# hashCheck.reinitialize_specific(list(flagged_files.keys()))


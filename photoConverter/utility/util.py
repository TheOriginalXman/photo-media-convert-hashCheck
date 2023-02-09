import mimetypes
import hashlib
import json
import os


def determine_file_type(file_path):
    return mimetypes.guess_type(file_path)[0]


def get_file_hash(file_path):
    # Get the file's hash
    h = hashlib.sha256()
    with open(file_path, 'rb') as file:
        while True:
            chunk = file.read(4096)
            if not chunk:
                break
            h.update(chunk)
    file.close()
    return h.hexdigest()

def validatePaths(config):
    for attribute in config.items():
        if attribute == 'exclusions':

            config[attribute]["paths"] = _getValidPathList(config[attribute]["paths"])

        elif attribute == "rootFolderList":
            config[attribute] = _getValidPathList(config[attribute])

        elif attribute in ["logFolderParentFolderPath", "convertedFolderParentFolderPath", "dbFileParentFolderPath"]:
            config[attribute] = _getSingleValidPath(config[attribute])
    return config

def _getSingleValidPath(attribute):
    valid = _getValidPathList([attribute])
    if len(valid) > 0:
        return valid[0]
    else:
        return ""

def _getValidPathList(listOfPaths):
    paths = []
    for path in listOfPaths:
        if os.path.exists(path):
            paths.append(path)
        else:
            # print("skipping path '{}', path cannot be found".format(path))
            pass
    return paths

def validateConfig(config_file_path):
    # Open the JSON config file
    try:
        with open(config_file_path, 'r') as file_db:
            config = json.load(file_db)
        file_db.close()
        #validateStructure()
        config = validatePaths(config)
        
        return config
    except IOError as e:
        print("Error loading configuration file: " + str(e))

    except Exception as e:
        print("Error loading configuration file: " + str(e))

def get_configurations(config_file_path):
    config = validateConfig(config_file_path)

    return _remove_duplicates_from_lists(config)

def _remove_duplicates_from_lists(config):
    extensions = config.get('exclustions',{}).get('extensions',[])
    if extensions:
        config['exclusions']['extensions'] = list(set(extensions))
    
    folderNames = config.get('exclustions',{}).get('folderNames',[])
    if folderNames:
        config['exclusions']['folderNames'] = list(set(folderNames))

    fileNames = config.get('exclustions',{}).get('fileNames',[])
    if folderNames:
        config['exclusions']['fileNames'] = list(set(fileNames))

    return config
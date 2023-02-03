import mimetypes
import hashlib
import json
import os


def determine_file_type(file_path):
    return mimetypes.guess_type(file_path)[0]


def get_file_hash(file_path):
    # Get the file's hash
    with open(file_path, 'rb') as f:
        file_hash = hashlib.sha256(f.read()).hexdigest()

    return file_hash

def validatePaths(config):
    for section, attributes in config.items():
        for attribute in attributes:
            if attribute == 'exclusions':

                config[section][attribute]["paths"] = _getValidPathList(config[section][attribute]["paths"])

            elif attribute == "rootFolderList":
                config[section][attribute] = _getValidPathList(config[section][attribute])

            elif attribute in ["logFolderParentFolderPath", "convertedFolderParentFolderPath", "dbFileParentFolderPath"]:
                config[section][attribute] = _getSingleValidPath(config[section][attribute])
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
            print("skipping path '{}', path cannot be found".format(path))
    return paths

def validateConfig(config_file_path):
    # Open the JSON config file
    try:
        with open(config_file_path, 'r') as file_db:
            config = json.load(file_db)

        #validateStructure()
        config = validatePaths(config)

        return config
    except IOError as e:
        print("Error loading configuration file: " + str(e))

    except Exception as e:
        print("Error loading configuration file: " + str(e))

def get_configurations(config_file_path, section):
    config = validateConfig(config_file_path)

    return mergeConfig(config, section)
    
def mergeConfig(config, section):
    globalConfig = config["global"]
    sectionConfig = config[section]
    mergedConfig = _merge_dicts(globalConfig, sectionConfig)

    return mergedConfig


def _merge_dicts(global_dict, local_dict):
    result = {}
    for key in global_dict:
        if key in local_dict:
            if isinstance(global_dict[key], list):
                result[key] = global_dict[key] + local_dict[key]
            elif isinstance(global_dict[key], dict):
                result[key] = _merge_dicts(global_dict[key], local_dict[key])
            elif local_dict[key]:
                result[key] = local_dict[key]
            else:
                result[key] = global_dict[key]
        else:
            result[key] = global_dict[key]
    for key in local_dict:
        if key not in result:
            result[key] = local_dict[key]
    return result

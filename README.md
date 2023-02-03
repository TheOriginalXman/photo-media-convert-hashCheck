## config.json documentation

1. Settings priority
   
   In most of the settings, the global value will be merged with the value of the individual files. ex.

   ''' 
        "global" : {
            "exclusions" : {
                "extensions" : [],
                "folderNames" : ["JPG Converted Folder", "MP4 Video Converted Folder"],
                "fileNames" : ["File_DB.db",".DS_Store"],
                "paths" : []
            }
        
        },
        "video" : {
            "exclusions" : {
                "extensions" : [.mov],
                "folderNames" : [test_folder],
                "fileNames" : ["33445.mov"],
                "paths" : []
            }
        }
    '''

    will be merged to a 'exclusions' attribute that looks like the following:

    '''
        "exclusions" : {
            "extensions" : [.mov],
             "folderNames" : [test_folder,"JPG Converted Folder", "MP4 Video Converted Folder"],
             "fileNames" : ["33445.mov","File_DB.db",".DS_Store"],
             "paths" : []
        }
    '''

    The only setting that will not merge is the logFolder. If a logFolder is specified anywhere other than the global than for that specific converter, it will be the location and the global value will be ignored. 

2. Paths
    All Paths must be absolute.
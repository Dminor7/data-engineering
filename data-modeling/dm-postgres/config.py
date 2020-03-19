from configparser import ConfigParser

def config(filename = 'database.ini',section = 'postgresql'):
    parser = ConfigParser()

    # Read the file
    parser.read(filename)

    # config-payload
    db = {}

    # Check for the section in the file and get parameters

    if(parser.has_section(section)):
        params = parser.items(section)

        for param in params:
            db[param[0]] = param[1]
        
    else:
        raise Exception('Section {0} not in found in the filename {1}'.format(section,filename))

    return db


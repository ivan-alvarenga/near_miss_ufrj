from simpledbf import Dbf5
import os


def add_content_to_mysql() -> bool:
    return True

def get_dbf_filenames(path='./') -> list:
    """ Will return a list of dbf filenames with absolute path on the path """
    
    print('checking for directory ' + path)
    
    items = os.listdir(path)
    dbf_list = []

    for item in items:
        
        _, file_extension = os.path.splitext(item)        
        if '.dbf' == file_extension:
            if path[-1] != '/':
                path += '/'
            dbf_list.append(path + item)
    
    if len(dbf_list) == 0:
        print('No item was found with dbf extension.')

    return dbf_list

def convert_to_csv(dbf_list, path='./') -> None:
    """ Crete files inside csv folder """

    # for each dbf file
    for dbf_filename in dbf_list:        
        #   read
        dbf = Dbf5(dbf_filename, codec='utf-8')
        filename, _ = os.path.splitext(dbf_filename)
        #   convert to csv
        print(filename)
        #dbf.to_csv('')
    
    return None

if __name__ == '__main__':

    working_path = '/home/user4/Data/ufrj/ic/SP/2015'
    # - get all dbf file in current directory
    dbfs = get_dbf_filenames(working_path)

    # to csv
    convert_to_csv(dbfs)

    #   add data to mysql db
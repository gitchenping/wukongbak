import configparser
kw={'db':'base','collection':'coll'}
def loadenv(**kwargs):

    databasename=kwargs['db']
    collectionname=kwargs['collection']
    a=123
    def wrap_o(func):
        def wrap(**args):

            if args.__contains__('database') and args['database'] is not None:
                database=args['database']
            else:
                database=databasename

            print('in wrap')
            collection='456'
            print(a)
            return func(database,collection)
        return wrap
    return wrap_o
    pass

@loadenv(db='base',collection='coll')
def test(database=None,collection=None):
    print('in test')
    print(database+collection)

def readconfini(path=''):
    cf = configparser.ConfigParser()
    allpath="./config.ini"
    cf.read(allpath, encoding='utf-8')
    return cf

config=readconfini()
if not config.has_option('db_hive','db_collection'):
    print('ok')
# test(database='1',collection='2')
# test(collection='2')
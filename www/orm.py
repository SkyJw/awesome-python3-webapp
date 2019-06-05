import asyncio
import datetime

async def create_pool(loop, **kw):
    logging.info('create database connection pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = await cur.fetchmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        logging.info('rows returned: %s' % len(rs))
        return rs

async def execute(sql, args):
    logging.info(sql)
    async with __pool.get() as conn:
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected =cur.rowcount
        except BaseException as e:
            raise
        return affected;

def create_args_string(num):
    L = ['?' for n in range(num)]
    return ','.join(L)

class Field(object):
    def __init__(self, name, column_type, primary_key,default):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default

    def __str__(self):
        return '<%s, %s : %s>' % (self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
    def __init__(self, name = None, primary_key = False, default = None, ddl = 'varchar(100)'):
        super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):
    def __init__(self, name = None, default = False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):
    def __init__(self, name = None, primary_key = False, default = 0):
        super().__init__(name, 'bigint', False, default)

class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)
        
class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)  


class ModelMetaclass(type):
    def __new__(cls, name, bases, attrs):#类的属性以字典形式存储在attrs中，key全部为字符串
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)#如果是Model类，则不需要通过元类来创建
        tableName = attrs.get('__table__', None) or name#如果没有定义__table__,则使用name作tablename
        logging.info('found model: %s (table : %s)' % (name, tableName))
        mapping = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items(): #遍历字段属性
            if isinstance(v, Field):
                logging.info(' found mapping: %s ==> %s' % (k, v)) 
                mapping[k] = v #又用一个字典，存储了table中的字段映射关系
                if v.primary_key:
                    if primaryKey:
                        raise StandardError('Duplicate primary key for field: %s' % k)
                    primaryKey = k
                else:
                    fields.append(k)

        if not primaryKey:
            raise StandardError('primary key not found.')
        for k in mapping.keys():
            attrs.pop(k) #将已经保留在mapping中的映射关系，在model子类中去除

        escaped_fields = list(map(lambda f: '`%s`' % f, fields)) #返回一个字符串列表，表中是形如'`name`'的字符串

        attrs['__mapping__'] = mapping #保存属性和列的映射关系
        attrs['__table__'] = tableName
        attrs['__primary_key__'] = primaryKey #主键的属性名
        attrs['__fields__'] = fields #除主键外的所有字段的属性名
        #使用字符串参数组合出sql语句
        attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
        attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
        attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
        attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
        return type.__new__(cls, name, bases, attrs)

#class Model(dict, metaclass = ModelMetaclass):
class Model(dict):

    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key): # 可以使用Model.key的方式来访问该字典
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Model' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if field.default is not None:
            if value is None: #不是model的直接属性，则是表的某一字段
                field = self.__mapping__[key]
                value = field.default() if callable(field.default) else field.default
                logging.debug('using default value for %s: %s' % (key, str(value)))
                setattr(self, key, value)
        return value
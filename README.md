# kvs
A simple C++ key value store on top of sqlite3, useful for application configurations.

## There's a callback incase the database won't open or has read errors:
`typedef void(*KVS_ERROR_CALLBACK) (void* p_object);`

## Create a new KeyValueStore to manage key/values like this:
{
CKeyValueStore* mp_config = new CKeyValueStore(configPath.c_str(), err_callback, err_callback_data);
mp_config->Init();
}

The db is lazy loaded, upon first read/write of a key/value. The error callback is called when the lazy loading has issues. 

When reading a key/value a default value is given in case that key does not exist, for example because the db failed to load. 

Values are maintained as strings, with binary data maintained as both raw bytes and base64. The base64 is written to the db.

## Utility methods:
`std::string base64_encode(unsigned char const* bytes_to_encode, uint32_t len);
std::string base64_decode(std::string const& s);
bool isKey( std::string& key );`

## Reading key methods:
`bool        ReadBool(   std::string& key, bool     defaultValue );
int32_t     ReadInt(    std::string& key, int32_t   defaultValue );
float       ReadReal(   std::string& key, float  defaultValue );
std::string ReadString( std::string& key, char*    defaultValue );
uint8_t*    ReadBinary( std::string& key, uint8_t* defaultValuePtr, uint32_t byte_size );`

## Writing key methods:
`char*    WriteString( std::string& key, char*    value );
bool     WriteBool(   std::string& key, bool     value );
int32_t  WriteInt(    std::string& key, int32_t   value );
float    WriteReal(   std::string& key, float  value );
uint8_t* WriteBinary( std::string& key, uint8_t* valuePtr, uint32_t byte_size );`

## write db to disk:
`bool SyncToDiskStorage(bool doNotInit = false);`		

## delete a key:
`bool DeleteKey( std::string& key );`

## delete keys starting with string:
`int32_t DeleteKeysStartingWith( std::string& keyPrefix );`


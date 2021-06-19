# kvs
A simple C++ key value store on top of sqlite3, useful for application configurations.

The basic idea is a std::map like facility one can put booleans, integers, floats, strings, and binary blobs into, each 
with a unique user defined string, the key, used for retrieval. The facility is called a "store", and it allows the user
to store any amount of keyed data. When created a key/value is maintained in memory, with an sqlite3 backing database. 
Binary data is maintained in RAM as binary, but as base64 when written to the db. 
There is also an optional, simplistic encryption subsystem; simple enough for easy replacement, and good enough to stop scrip-kiddies.  

## There's a callback incase the database won't open or has read errors:
`typedef void(*KVS_ERROR_CALLBACK) (void* p_object);`

## Create a new KeyValueStore to manage key/values like this:
```
CKeyValueStore* mp_config = new CKeyValueStore(configPath.c_str(), err_callback, err_callback_data);
mp_config->Init();
```

The db is lazy loaded, upon first read/write of a key/value. The error callback is called when the lazy loading has issues.
If the db has load issues, the provided default values are used for the keyValeyStore's operation. 

When reading a key/value a default value is given in case that key does not exist, for example because the db failed to load. 

Values are maintained as strings, with binary data maintained as both raw bytes and base64. The base64 is written to the db.

## Utility methods:
```
std::string base64_encode(unsigned char const* bytes_to_encode, uint32_t len);
std::string base64_decode(std::string const& s);
bool isKey( std::string& key );
```

## Reading key methods:
```
bool        ReadBool(   std::string& key, bool     defaultValue );
int32_t     ReadInt(    std::string& key, int32_t   defaultValue );
float       ReadReal(   std::string& key, float  defaultValue );
std::string ReadString( std::string& key, char*    defaultValue );
uint8_t*    ReadBinary( std::string& key, uint8_t* defaultValuePtr, uint32_t byte_size );
```

## Writing key methods:
```
char*    WriteString( std::string& key, char*    value );
bool     WriteBool(   std::string& key, bool     value );
int32_t  WriteInt(    std::string& key, int32_t   value );
float    WriteReal(   std::string& key, float  value );
uint8_t* WriteBinary( std::string& key, uint8_t* valuePtr, uint32_t byte_size );
```

## write db to disk:
`bool SyncToDiskStorage(bool doNotInit = false);`		

## delete a key:
`bool DeleteKey( std::string& key );`

## delete keys starting with string:
`int32_t DeleteKeysStartingWith( std::string& keyPrefix );`


////////////////////////////////////////////////////////////////////////////
// Name:        kvs.cpp
// Purpose:     a simple key value storage facility
// Author:      Blake Senftner
// Created:     04/18/2014 
/////////////////////////////////////////////////////////////////////////////


#include "kvs.h"

#define ACTUALLY_DO_ENCRYPTION (0)   // if false, encryption does not happen

///////////////////////////////////////////////////////////////////////////////////
CKeyValue::CKeyValue( const char* keyStr, const char* valueStr )
{
	m_key = keyStr;
	m_value = valueStr;
	mp_binaryData = NULL;
	m_binarySize = 0;
}

//////////////////////////////////////////////////////////////////////////////////////////
// used with keys that own structures as values:
//		valueStr is expected to be the binary data base64 encoded
//		value is expected to be a pointer to binary data with the same values as valueStr
//    byte_size is expected to be the byte size of the data pointed to by value
CKeyValue::CKeyValue( const char* keyStr, std::string& valueStr, uint8_t* value, uint32_t byte_size )
{
	m_key = keyStr;
	m_value = valueStr;
	mp_binaryData = (uint8_t*)malloc( sizeof(uint8_t) * byte_size );
	if ( mp_binaryData != NULL )
	{ 
		m_binarySize = byte_size;
		memcpy( mp_binaryData, value, byte_size );
	}
}

CKeyValue::~CKeyValue() {}
///////////////////////////////////////////////////////////////////////////////////


///////////////////////////////////////////////////////////////////////////////////
CKeyValueStore::CKeyValueStore( const char* keyValueStorePath, KVS_ERROR_CALLBACK cb, void* cb_data )
{
	mp_error_callback = cb;
	mp_error_object = cb_data;

	m_path   = keyValueStorePath;
	std::string basePath = GetPath( m_path );

	mp_db = NULL;		

	m_state = -1; // created

	m_readBinaryErrorState = 0;
	m_writeBinaryErrorState = 0;
	
	// when working with a private compile of this code, change this for weak but okay
	// encryption on the usernames and passwords embedded in ip cam urls and email settings:
	m_passkey = "zg[dh$K;r.,!~YJ[db*qGxY2_JQwnrWh(;%Nk+'7ebqb,J`#w&^C[4T]_,+(GVZt^g7n7ha3n?DE[R_"; 
	// be sure to check ACTUALLY_DO_ENCRYPTION !!
	// Actually, if you really care about encryption, replace the encrypt/decrypt logic with your own.
}

///////////////////////////////////////////////////////////////////////////////////
CKeyValueStore::~CKeyValueStore()
{
	if (m_state == 0)
	{
		SyncToDiskStorage(false);

		// spin through getting rid of any allocated binary data and each map element:
		std::map<std::string, CKeyValue>::iterator it = m_pairs.begin();
		while (it != m_pairs.end())
		{
			CKeyValue& kv = it->second;
			if (kv.mp_binaryData)
			{
				free(kv.mp_binaryData);
				kv.mp_binaryData = NULL;
			}

			it = m_pairs.erase(it);
		}

	}

	 if (mp_db) sqlite3_close(mp_db);
}

////////////////////////////////////////////////////////////////////////////////
std::string CKeyValueStore::GetPath(std::string& s)
{
	std::string p;
	size_t found = s.find_last_of("/\\");
	if (found != std::string::npos)
	{
		p = s.substr(0, found);
	}
	return p;
}

////////////////////////////////////////////////////////////////////////////////
#include <boost/filesystem.hpp>

bool CKeyValueStore::VerifyCreateDirectory(std::string& directory)
{
	// get the directory inside a boost::filesystem::path
	boost::filesystem::path dir(directory);

	// verify the directory exists:
	if (!boost::filesystem::is_directory(dir))
	{
		// we need to create the directory:
		if (!boost::filesystem::create_directory(dir))
		{
			return false;
		}
	}

	return true;
}

////////////////////////////////////////////////////////////////////
bool CKeyValueStore::DeleteKey( std::string& key )
{
	std::map<std::string, CKeyValue>::iterator it = m_pairs.find(key);
	if (it == m_pairs.end())
		 return false;					// key did not exist

	RemoveKeyFromDB(key);			// remove from disk cache
	m_pairs.erase(it);				// remove from RAM cache

	return true;
}

////////////////////////////////////////////////////////////////////
int32_t CKeyValueStore::DeleteKeysStartingWith(std::string& keyPrefix )
{
	int32_t deleted_key_count = 0;
	int32_t prefix_len = (int32_t)keyPrefix.size();

	// spin through...
	std::map<std::string, CKeyValue>::iterator it = m_pairs.begin();
	while (it != m_pairs.end())
	{
		CKeyValue& kv = it->second;
		if (kv.m_key.compare( 0, prefix_len, keyPrefix ) == 0)
		{
			RemoveKeyFromDB(kv.m_key);	// remove from disk cache
			it = m_pairs.erase(it);			// remove from RAM cache
			deleted_key_count++;
		}
		else it++;
	}

	return deleted_key_count;
}

///////////////////////////////////////////////////////////////////////////////////
uint8_t* CKeyValueStore::encrypt( uint8_t* msg, uint32_t msg_len, std::string const&key )
{
	if (ACTUALLY_DO_ENCRYPTION == false)
		 return msg;

	if (key.empty())
     return msg;
    
	int32_t key_size = (int32_t)key.size();
  for (uint32_t i = 0; i < msg_len; i++)
      msg[i] ^= key[ i%key_size ];
  
	return msg;
}

///////////////////////////////////////////////////////////////////////////////////
uint8_t* CKeyValueStore::decrypt( uint8_t* msg, uint32_t msg_len, std::string const&key )
{
	return encrypt( msg, msg_len, key );
}

///////////////////////////////////////////////////////////////////////////////////////////////
// read the text file whose path is given at creation
// it should contain a series of key/value pairs, each separated by an equals sign, no spaces
// This returns 0 for okay, or 1 if critical errors occured.
// This parses the key/value pairs into a vector for easy searching later, and while doing so
// counts how many key/value pairs failed to parse - which is not considered a critical error.
///////////////////////////////////////////////////////////////////////////////////////////////
int32_t CKeyValueStore::Init( void )
{
	if (!this)
		 return 1;

	// if just created, not initialized yet
	if (m_state == -1) 
	{
		if (OpenDB( m_path.c_str() ))
		{
			ReadKeyValueStoreFromDisk();
		}
		else
		{
			m_state = 1;	// means read error
		}
	}
	
	return m_state;
}

///////////////////////////////////////////////////////////////////////////////////
int32_t CKeyValueStore::GetStatus( void )
{
	return m_state;
}

///////////////////////////////////////////////////////////////////////////////////
void CKeyValueStore::LazyInit( void )
{
	if (!this) 
		return;

	// lazy init:
	int32_t state = Init();
	// had a problem reading the config file?
	if (state == 1)
	{
		if (mp_error_callback)
		{
			state = 2; // un mumento...
			mp_error_callback(mp_error_object);
		}
		m_state = 0;	// if not cleared, the config file won't save.
	}
}


///////////////////////////////////////////////////////////////////////////////////////////
// returns true if the passed string is a numeric string, if so value is set to that value
bool CKeyValueStore::isParam( std::string& valueStr, int32_t& value )
{
	char* p;
  value = strtol(valueStr.c_str(), &p, 0);
  return *p == 0;
}

///////////////////////////////////////////////////////////////////////////////////
// returns true if the passed string is a float, if so value is set to that float
bool CKeyValueStore::isParam( std::string& valueStr, float& value )
{
	char* p;
  value = (float)strtod(valueStr.c_str(), &p);
  return *p == 0;
}

///////////////////////////////////////////////////////////////////////////////////
bool CKeyValueStore::isKey( std::string& key )
{
	LazyInit(); // even if LazyInit fails, we continue...

	std::map<std::string, CKeyValue>::iterator it = m_pairs.find(key);
	if (it == m_pairs.end())
		 return false;					// key did not exist

	return true;
}

///////////////////////////////////////////////////////////////////////////////////
bool CKeyValueStore::ReadBool( std::string& key, bool defaultValue )
{
	LazyInit(); // even if LazyInit fails, we continue...

	// Create an iterator of map
	std::map<std::string, CKeyValue>::iterator it;

	// Find the element with key:
	it = m_pairs.find(key);

	// Check if element exists in map or not
	if (it != m_pairs.end()) 
	{
		CKeyValue kv = it->second;

		// expecting the value to be a zero or one, it could be anything:
		int32_t numVal;
		// returns true if string is numerical, 
		// and if true sets numVal to that numerical value:
		if (isParam(kv.m_value, numVal))
		{
			return (numVal != 0); // allow misuse of numerical, nonboolen settings as booleans
		}
		else
		{
			// value is not numerical, so return the default value:
			return defaultValue;
		}
	}
	else 
	{
		// the key was not found, so it is created:
		const char *boolStrVal = (defaultValue) ? "1" : "0";
		//
		CKeyValue kv(key.c_str(), boolStrVal);
		//
		m_pairs.insert(std::make_pair(key, kv));		// insert into RAM cache
		// SetValToDB( kv );														// insert into DB cache
	}

	return defaultValue;
}

///////////////////////////////////////////////////////////////////////////////////
int32_t CKeyValueStore::ReadInt( std::string& key, int32_t defaultValue )
{
	LazyInit(); // even if LazyInit fails, we continue...

	// Create an iterator of map
	std::map<std::string, CKeyValue>::iterator it;

	// Find the element with key:
	it = m_pairs.find(key);

	// Check if element exists in map or not
	if (it != m_pairs.end()) 
	{
		CKeyValue kv = it->second;

		// expecting the value to be a number, it could be anything:
		int32_t numVal;
		// returns true if string is numerical, 
		// and if true sets numVal to that numerical value:
		if (isParam( kv.m_value, numVal ))
		{
			return numVal;
		}
		else
		{
			// value is not numerical, so return the default value:
			return defaultValue;
		}
	}
	else
	{
		// the key was not found, so it is created:
		std::string valueStr = std::to_string(defaultValue);
		//
		CKeyValue kv( key.c_str(), valueStr.c_str() );
		//
		m_pairs.insert(std::make_pair(key, kv));		// insert into RAM cache
		// SetValToDB( kv );														// insert into DB cache
	}

	return defaultValue;
}

///////////////////////////////////////////////////////////////////////////////////
float CKeyValueStore::ReadReal( std::string& key, float defaultValue )
{
	LazyInit(); // even if LazyInit fails, we continue...

	// Create an iterator of map
	std::map<std::string, CKeyValue>::iterator it;

	// Find the element with key:
	it = m_pairs.find(key);

	// Check if element exists in map or not
	if (it != m_pairs.end()) 
	{
		CKeyValue kv = it->second;

		// expecting the value to be a float, it could be anything:
		float numVal;
		// returns true if string is a float, 
		// and if true sets numVal to that floating point value:
		if (isParam( kv.m_value, numVal ))
		{
			return numVal;
		}
		else
		{
			// value is not a float, so return the default value:
			return defaultValue;
		}
	}
	else
	{
		// the key was not found, so it is created:
		std::string valueStr = std::to_string(defaultValue);
		//
		CKeyValue kv( key.c_str(), valueStr.c_str() );
		//
		m_pairs.insert(std::make_pair(key, kv));		// insert into RAM cache
		// SetValToDB( kv );														// insert into DB cache
	}

	return defaultValue;
}

///////////////////////////////////////////////////////////////////////////////////
std::string CKeyValueStore::ReadString( std::string& key, char* defaultValue )
{
	LazyInit(); // even if LazyInit fails, we continue...

	// Create an iterator of map
	std::map<std::string, CKeyValue>::iterator it;

	// Find the element with key:
	it = m_pairs.find(key);

	// Check if element exists in map or not
	if (it != m_pairs.end()) 
	{
		CKeyValue kv = it->second;
		
		return kv.m_value;
	}
	else
	{
		// the key was not found, so it is created:
		CKeyValue kv( key.c_str(), defaultValue );
		//
		m_pairs.insert(std::make_pair(key, kv));		// insert into RAM cache
		// SetValToDB( kv );														// insert into DB cache
	}

	return std::string( defaultValue );
}

///////////////////////////////////////////////////////////////////////////////////
static const std::string gBase64_chars = 
             "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
             "abcdefghijklmnopqrstuvwxyz"
             "0123456789+/";

///////////////////////////////////////////////////////////////////////////////////
std::string CKeyValueStore::base64_encode(unsigned char const* bytes_to_encode, uint32_t in_len)
{
	std::string ret;
  int32_t i = 0;
  int32_t j = 0;
  unsigned char char_array_3[3];
  unsigned char char_array_4[4];

  while (in_len--) 
	{
    char_array_3[i++] = *(bytes_to_encode++);
    if (i == 3) 
		{
      char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
      char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
      char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
      char_array_4[3] = char_array_3[2] & 0x3f;

      for(i = 0; (i <4) ; i++)
        ret += gBase64_chars[char_array_4[i]];
      i = 0;
    }
  }

  if (i)
  {
    for(j = i; j < 3; j++)
      char_array_3[j] = '\0';

    char_array_4[0] = (char_array_3[0] & 0xfc) >> 2;
    char_array_4[1] = ((char_array_3[0] & 0x03) << 4) + ((char_array_3[1] & 0xf0) >> 4);
    char_array_4[2] = ((char_array_3[1] & 0x0f) << 2) + ((char_array_3[2] & 0xc0) >> 6);
    char_array_4[3] = char_array_3[2] & 0x3f;

    for (j = 0; (j < i + 1); j++)
      ret += gBase64_chars[char_array_4[j]];

    while((i++ < 3))
      ret += '=';
  }

  return ret;
}

///////////////////////////////////////////////////////////////////////////////////
std::string CKeyValueStore::base64_decode(std::string const& encoded_string)
{
	int32_t in_len = (int32_t)encoded_string.size();
  int32_t i = 0;
  int32_t j = 0;
  int32_t in_ = 0;
  unsigned char char_array_4[4], char_array_3[3];
  std::string ret;

  while (in_len-- && ( encoded_string[in_] != '=') && is_base64(encoded_string[in_])) 
	{
    char_array_4[i++] = encoded_string[in_]; in_++;
    if (i == 4) 
		{
      for (i = 0; i <4; i++)
        char_array_4[i] = (char)gBase64_chars.find(char_array_4[i]);

      char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
      char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
      char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

      for (i = 0; (i < 3); i++)
        ret += char_array_3[i];
      i = 0;
    }
  }

  if (i) 
	{
    for (j = i; j <4; j++)
      char_array_4[j] = 0;

    for (j = 0; j <4; j++)
      char_array_4[j] = (char)gBase64_chars.find(char_array_4[j]);

    char_array_3[0] = (char_array_4[0] << 2) + ((char_array_4[1] & 0x30) >> 4);
    char_array_3[1] = ((char_array_4[1] & 0xf) << 4) + ((char_array_4[2] & 0x3c) >> 2);
    char_array_3[2] = ((char_array_4[2] & 0x3) << 6) + char_array_4[3];

    for (j = 0; (j < i - 1); j++) ret += char_array_3[j];
  }

  return ret;
}

///////////////////////////////////////////////////////////////////////////////////
uint8_t* CKeyValueStore::ReadBinary( std::string& key, uint8_t* defaultValue, uint32_t byte_size )
{
	LazyInit(); // even if LazyInit fails, we continue...

	// Create an iterator of map
	std::map<std::string, CKeyValue>::iterator it;

	// Find the element with key:
	it = m_pairs.find(key);

	// Check if element exists in map or not
	if (it != m_pairs.end()) 
	{
		CKeyValue kv = it->second;

		// because this is binary data encoded as base64, it needs storage for the decoded version.
		// That decoded version is created upon first ReadBinary(). Look for it first:
		if (kv.m_binarySize)
		{
			// already decoded this one, so just return that:
			return kv.mp_binaryData;
		}

		std::string rawDecode = base64_decode( kv.m_value );

		if ( rawDecode.size() != byte_size )
		{
			assert( 1 );
		}

		kv.mp_binaryData = (uint8_t*)malloc( sizeof(uint8_t) * byte_size );
		if ( kv.mp_binaryData != NULL )
		{
			kv.m_binarySize = byte_size;
			memcpy( kv.mp_binaryData, rawDecode.c_str(), byte_size );
		}
		return kv.mp_binaryData;
	}


	// the key was not found, so it is created:
	std::string base64_version = base64_encode((uint8_t*)defaultValue, byte_size);
	CKeyValue kv( (const char *)key.c_str(), base64_version, defaultValue, byte_size );
	//
	m_pairs.insert(std::make_pair(key, kv));		// insert into RAM cache
	// SetValToDB( kv );														// insert into DB cache


	return defaultValue;
}

///////////////////////////////////////////////////////////////////////////////////
bool CKeyValueStore::WriteBool( std::string& key, bool value )
{
	LazyInit(); // even if LazyInit fails, we continue...

	// Create an iterator of map
	std::map<std::string, CKeyValue>::iterator it;

	// prevent other threads from changing our data during this operation:
	std::lock_guard<std::mutex> guard(m_mutex);

	// Find the element with key:
	it = m_pairs.find(key);
	if (it != m_pairs.end()) 
	{
		CKeyValue& kv = it->second;
		kv.m_value = (value) ? "1" : "0";
		// if (!delayWrite) { SetValToDB( kv ); }
	}
	else
	{
		// the key was not found, so it is created:
		const char *boolStrVal = (value) ? "1" : "0";
		//
		CKeyValue kv(key.c_str(), boolStrVal);
		//
		m_pairs.insert(std::make_pair(key, kv));		// insert into RAM cache
		// if (!delayWrite) { SetValToDB( kv ); }
	}

	return value;
}

///////////////////////////////////////////////////////////////////////////////////
int32_t CKeyValueStore::WriteInt( std::string& key, int32_t value )
{	
	LazyInit(); // even if LazyInit fails, we continue...

	// Create an iterator of map
	std::map<std::string, CKeyValue>::iterator it;

	// prevent other threads from changing our data during this operation:
	std::lock_guard<std::mutex> guard(m_mutex);

	// Find the element with key:
	it = m_pairs.find(key);
	if (it != m_pairs.end()) 
	{
		CKeyValue& kv = it->second;
		kv.m_value = std::to_string(value);
		// if (!delayWrite) { SetValToDB( kv ); }
	}
	else
	{
		// the key was not found, so it is created:
		CKeyValue kv(key.c_str(), std::to_string(value).c_str());
		//
		m_pairs.insert(std::make_pair(key, kv));		// insert into RAM cache
		// if (!delayWrite) { SetValToDB( kv ); }
	}

	return value;
}

///////////////////////////////////////////////////////////////////////////////////
float CKeyValueStore::WriteReal( std::string& key, float value )
{
	LazyInit(); // even if LazyInit fails, we continue...

	// Create an iterator of map
	std::map<std::string, CKeyValue>::iterator it;

	// prevent other threads from changing our data during this operation:
	std::lock_guard<std::mutex> guard(m_mutex);

	// Find the element with key:
	it = m_pairs.find(key);
	if (it != m_pairs.end()) 
	{
		CKeyValue& kv = it->second;
		kv.m_value = std::to_string(value);
		// if (!delayWrite) { SetValToDB( kv ); }
	}
	else
	{
		// the key was not found, so it is created:
		CKeyValue kv(key.c_str(), std::to_string(value).c_str());
		//
		m_pairs.insert(std::make_pair(key, kv));		// insert into RAM cache
		// if (!delayWrite) { SetValToDB( kv ); }
	}

	return value;
}

///////////////////////////////////////////////////////////////////////////////////
char* CKeyValueStore::WriteString( std::string& key, char* value )
{
	LazyInit(); // even if LazyInit fails, we continue...

	// Create an iterator of map
	std::map<std::string, CKeyValue>::iterator it;

	// prevent other threads from changing our data during this operation:
	std::lock_guard<std::mutex> guard(m_mutex);

	// Find the element with key:
	it = m_pairs.find(key);
	if (it != m_pairs.end()) 
	{
		CKeyValue& kv = it->second;
		kv.m_value = value;
		// if (!delayWrite) { SetValToDB( kv ); }
	}
	else
	{
		// the key was not found, so it is created:
		CKeyValue kv(key.c_str(), value);
		//
		m_pairs.insert(std::make_pair(key, kv));		// insert into RAM cache
		// if (!delayWrite) { SetValToDB( kv ); }
	}

	return value;
}

///////////////////////////////////////////////////////////////////////////////////
uint8_t* CKeyValueStore::WriteBinary( std::string& key, uint8_t* valuePtr, uint32_t byte_size )
{
	LazyInit(); // even if LazyInit fails, we continue...

	// Create an iterator of map
	std::map<std::string, CKeyValue>::iterator it;

	// prevent other threads from changing our data during this operation:
	std::lock_guard<std::mutex> guard(m_mutex);

	// Find the element with key:
	it = m_pairs.find(key);
	if (it != m_pairs.end()) 
	{
		CKeyValue& kv = it->second;

		// binary data is stored both as it's raw bytes and as a base64 encoded string

		// here I insure the binary byte storage is the correct size: 
		if (kv.mp_binaryData)
		{
			if (kv.m_binarySize != byte_size)
			{
				free( kv.mp_binaryData );
				kv.mp_binaryData = (uint8_t*)malloc( sizeof(uint8_t) * byte_size );
			}
		}
		else
		{
			kv.mp_binaryData = (uint8_t*)malloc( sizeof(uint8_t) * byte_size );
		}

		// if we have storage for the binary data, copy here:
		if (kv.mp_binaryData)
		{
			memcpy( kv.mp_binaryData, valuePtr, byte_size );
		}

		// update the base64 encoded version:
		kv.m_value = base64_encode(valuePtr, byte_size).c_str();
		
		// if (!delayWrite) { SetValToDB( kv ); }
	}
	else
	{
		// the key was not found, so it is created:
		CKeyValue kv(key.c_str(), base64_encode(valuePtr, byte_size).c_str());
		//
		m_pairs.insert(std::make_pair(key, kv));		// insert into RAM cache
		// if (!delayWrite) { SetValToDB( kv ); }
	}
	return valuePtr;
}

///////////////////////////////////////////////////////////////////////////////////
bool CKeyValueStore::SyncToDiskStorage(bool doNotInit)
{
	if (!doNotInit)
		LazyInit(); // even if LazyInit fails, we continue...

	if (!mp_db) 
	{ 
		m_emsg = "SyncToDiskStorage() mp_db=0"; 
		return false; 
	};

	bool ok = true;
  std::string sql;
  sqlite3_stmt *statement;

  sql = "REPLACE INTO keyValueStore (key, value) VALUES (?1, ?2);";
  sqlite3_prepare_v2(mp_db, sql.c_str(), -1, &statement, NULL);

  sqlite3_exec(mp_db, "BEGIN TRANSACTION", NULL, NULL, NULL);

	// spin through...
	std::map<std::string, CKeyValue>::iterator it = m_pairs.begin();
	while (it != m_pairs.end())
	{
		CKeyValue& kv = it->second;
		
		sqlite3_bind_text(statement, 1, kv.m_key.c_str(),   -1, SQLITE_STATIC);
		sqlite3_bind_text(statement, 2, kv.m_value.c_str(), -1, SQLITE_STATIC);

		if (sqlite3_step(statement) != SQLITE_DONE)
		{
		  ok = false;
		}

		sqlite3_reset(statement);
		
		it++;
	}
  
  if (ok) sqlite3_exec(mp_db, "END TRANSACTION", NULL, NULL, NULL);
  else    sqlite3_exec(mp_db, "ROLLBACK TRANSACTION", NULL, NULL, NULL);

  sqlite3_finalize(statement);

	return ok;
}

///////////////////////////////////////////////////////////////////////////////////
int32_t CKeyValueStore::ReadKeyValueStoreFromDisk( void )
{
	m_readBinaryErrorState = 0;
	m_state = 0;

	int32_t pair_count = GetValFromDB("SELECT COUNT(key) FROM keyValueStore;");
	if (pair_count < 0)
	{
		m_readBinaryErrorState = 1;
	}
	else if (pair_count > 0)
	{
		bool					ok = true;
		std::string		sql;
		sqlite3_stmt	*statement;

		sql = "SELECT * FROM keyValueStore;";
		if (sqlite3_prepare_v2(mp_db, sql.c_str(), -1, &statement, NULL) != SQLITE_OK)
		{
		  m_emsg = std::string("ReadKeyValueStoreFromDisk() Prepare Error: ") + std::string(sqlite3_errmsg(mp_db));
		  return -1;
		}

		// Execute the statement and iterate over all the resulting rows.
		while (SQLITE_ROW == sqlite3_step(statement))
		{
			// Notice the columns have 0-based indices here.
			std::string key = reinterpret_cast<const char*>(sqlite3_column_text(statement, 0));
			std::string val = reinterpret_cast<const char*>(sqlite3_column_text(statement, 1));
			
			CKeyValue kv( key.c_str(), val.c_str() );

			m_pairs.insert( std::make_pair(key.c_str(),kv) );
		}
		// Clean up the select statement
		sqlite3_finalize(statement);

	}

	if (m_readBinaryErrorState)
		m_state = 1;

	return m_state;
}

////////////////////////////////////////////////////////////////////////////////
bool CKeyValueStore::ExecuteSQL(sqlite3* db, const char* sql, std::string& emsg)
{
  //fprintf(stderr, "%s\n", sql);
	char* ebuffer = NULL;
  int32_t rc = sqlite3_exec(db, sql, NULL, 0, &ebuffer);
  if (rc != SQLITE_OK)
  {
    emsg = std::string( ebuffer );
    sqlite3_free(ebuffer);
    return false;
  }
  return true;
}

////////////////////////////////////////////////////////////////////////////////
int32_t CKeyValueStore::GetValFromDB(const char* sql)
{
  if (!mp_db) { m_emsg = "GetValFromDB() mp_db=0"; return -1; };

  int32_t val = -1;

  sqlite3_stmt *selectStatement;
  if (sqlite3_prepare_v2(mp_db, sql, -1, &selectStatement, NULL) != SQLITE_OK)
  {
    m_emsg = std::string("GetValFromDB() Prepare Error: ") + std::string(sqlite3_errmsg(mp_db));
    return -1;
  }
  // Execute the statement and iterate over all the resulting rows.
  while (SQLITE_ROW == sqlite3_step(selectStatement))
  {
    // Notice the columns have 0-based indices here.
    val = (int32_t)sqlite3_column_int(selectStatement, 0);
  }
  // Clean up the select statement
  sqlite3_finalize(selectStatement);

  return val;
}

////////////////////////////////////////////////////////////////////////////////
int32_t CKeyValueStore::SetValToDB(const CKeyValue& keyValue)
{
  if (!mp_db) { m_emsg = "SetValToDB() mp_db=0"; return -1; };

	bool ok = true;
  std::string sql;
  sqlite3_stmt *statement;

  sql = "REPLACE INTO keyValueStore (key, value) VALUES (?1, ?2);";
  sqlite3_prepare_v2(mp_db, sql.c_str(), -1, &statement, NULL);

  sqlite3_exec(mp_db, "BEGIN TRANSACTION", NULL, NULL, NULL);

  sqlite3_bind_text(statement, 1, keyValue.m_key.c_str(),   -1, SQLITE_STATIC);
	sqlite3_bind_text(statement, 2, keyValue.m_value.c_str(), -1, SQLITE_STATIC);

  if (sqlite3_step(statement) != SQLITE_DONE)
  {
    ok = false;
  }

  sqlite3_reset(statement);
  
  if (ok) sqlite3_exec(mp_db, "END TRANSACTION", NULL, NULL, NULL);
  else sqlite3_exec(mp_db, "ROLLBACK TRANSACTION", NULL, NULL, NULL);
  sqlite3_finalize(statement);

	return ok;
}

////////////////////////////////////////////////////////////////////////////////
// removes an image from the DB (NOTE: if the image belopngs to a person with one image, the person is also deleted)
// returns -1 = error, 0 = key removed
////////////////////////////////////////////////////////////////////////////////
int32_t CKeyValueStore::RemoveKeyFromDB(std::string& key)
{
  if (!mp_db) { m_emsg = "RemoveKeyFromDB() m_db=0"; return -1; };
  
	std::string sql, msg;

  sql = std::string("DELETE FROM keyValueStore WHERE key = ") + key + ";";
  if (!ExecuteSQL(mp_db, sql.c_str(), msg))
  {
    m_emsg = std::string("RemoveKeyFromDB() ") + msg;
    return -1;
  }

  return 0;
}

////////////////////////////////////////////////////////////////////////////////
bool CKeyValueStore::CreateTables(void)
{
  if (!mp_db) { m_emsg = "CreateTables() mp_db=0"; return false; };

  std::string sql, msg;

  // keyValueStore table
  sql = "CREATE TABLE IF NOT EXISTS ";
  sql += "keyValueStore(";
  sql += "  key           TEXT PRIMARY KEY";
  sql += " ,value         TEXT";
  sql += ");";
  if (!ExecuteSQL(mp_db, sql.c_str(), msg)) 
	{ 
		m_emsg = std::string("CreateTables() ") + msg; 
		return false; 
	};

  return true;
}

/////////////////////////////////////////////////////////////////////////////
// opens or creates a given DB
/////////////////////////////////////////////////////////////////////////////
bool CKeyValueStore::OpenDB(const char* fname)
{
  // close if already open
  if (mp_db)
  {
    sqlite3_close(mp_db);
    mp_db = NULL;
  };

  // save name
  m_db_fname = fname;

  // ensure we have a folder to hold the DB
	std::string db_dir_on_disk(GetPath(m_db_fname));
  VerifyCreateDirectory(db_dir_on_disk);


  int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX;

  // open the database (creates if not existing)
  int32_t rc = sqlite3_open_v2(m_db_fname.c_str(), &mp_db, flags, NULL);
  if (rc != SQLITE_OK)
  {
    m_emsg = std::string("OpenDB() Can't open: ") + m_db_fname + sqlite3_errmsg(mp_db);
    sqlite3_close(mp_db);
    mp_db = NULL;
    return false;
  }

  std::string sql;
  
  // set cache size
  sql = "PRAGMA cache_size = -100000"; // according to docs, this should mean 100MB
  if (!ExecuteSQL(mp_db, sql.c_str(), m_emsg))
  {
    sqlite3_close(mp_db);
    mp_db = NULL;
    return false;
  }
  

  // enable foreign keys
  sql = "PRAGMA foreign_keys = ON";
  if (!ExecuteSQL(mp_db, sql.c_str(), m_emsg))
  {
    sqlite3_close(mp_db);
    mp_db = NULL;
    return false;
  }
  
  // faster disk access (DEV, might allow DB corruption if power outage at time of update)
  sql = "PRAGMA synchronous = OFF";
  if (!ExecuteSQL(mp_db, sql.c_str(), m_emsg))
  {
    sqlite3_close(mp_db);
    mp_db = NULL;
    return false;
  }
  
  if (!CreateTables()) 
		 return false;

  return true;
}


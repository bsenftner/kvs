/////////////////////////////////////////////////////////////////////////////
// Name:        kvs.h
// Purpose:     A key value storage facility using a butt simple sqlite key
//							value approach: "values" that can be put into a CKeyValueStore
//							include Bools/ints/floats, strings, or Binary Blobs. 
// 
//							Each value is stored by name, it's "key". Storing to the same
//							key twice overwrites the previous value. 
//
//							Be careful to take out the same type of data as put into a key, 
//							nothing prevents using values with the wrong type.
// 
//							Values are stored as strings: bools are "0" or "1",
//							ints are strings, floats use std::to_string(), and binary is
//							base64 encoded.  
// 
//							The db is lazy loaded, meaning it is not loaded until used.
//							When used, it is loaded into RAM and maintained as a std::map. 
//							When the CKeyValueStore is deleted, the sqlite db is committed 
//							to disk. 
//							Writing to the db can be triggered via SyncToDiskStorage() too.
// 
//							When initializing, an error callback can be passed that is only
//							called if the db has problems opening/reading.  
// 
//							Writing or reading to/from a key can create a key/value. When
//							reading from a key, a default value is required that is used
//							if the key does not exist. A key may not exist if never used
//							before, or if the db has problems opening/reading. 
// 
// Author:      Blake Senftner
// Created:     04/18/2014 
/////////////////////////////////////////////////////////////////////////////

#ifndef _KVS_H_ 
#define _KVS_H_ 

#define WIN32_LEAN_AND_MEAN      // Exclude rarely-used stuff from Windows headers
#include <windows.h>

#include <cstdio>
#include <string>
#include <vector>
#include <map>
#include <mutex>
#include <assert.h>
#include "base64.h"
#include "sqlite3.h"

class CKeyValue
{
public:
	CKeyValue( const char* keyStr, const char* valueStr );
	CKeyValue( const char* keyStr, std::string& valueStr, uint8_t* value, uint32_t byte_size );
	~CKeyValue(); 

	std::string	m_key;
	std::string	m_value;
	uint8_t*    mp_binaryData;
	uint32_t    m_binarySize;
};

typedef void(*KVS_ERROR_CALLBACK) (void* p_object);

class CKeyValueStore
{
public:
	// this is a lazy constructor: it accepts the path at create, but
	// does not use it until needed, throwing a path error then if need be:
	CKeyValueStore( const char* keyValueStorePath, KVS_ERROR_CALLBACK cb, void* cb_data );
	~CKeyValueStore();

	//
	// library error callback; if set this is called if a library error occurs
	void SetErrorCallBack(KVS_ERROR_CALLBACK p_func, void* p_object);
	//
	KVS_ERROR_CALLBACK  mp_error_callback;
	void* mp_error_object;

	// called automatically upon first config operation request
	// but may be called independantly after creation to verify the path is good
	int32_t Init( void );	

	std::string GetPath(std::string& s);
	bool VerifyCreateDirectory(std::string& directory);

	int32_t GetStatus( void );

	void LazyInit( void );

	bool DeleteKey( std::string& key );

	int32_t DeleteKeysStartingWith( std::string& keyPrefix );
	
	// various value strings are expected to be numerical, or capable of being reduced to numerical (bools)
	// this returns true if the passed string is a number, including hex and octal
	bool isParam( std::string& valueStr, int32_t& value );

	// various value strings are expected to be floating point numbers
	// this returns true if the passed string is a float, including scientific notation
	bool isParam( std::string& valueStr, float& value );
	
	bool isKey( std::string& key ); // return true if passed string is a key in the store

	inline bool is_base64(unsigned char c) 
	{
		return (isalnum(c) || (c == '+') || (c == '/'));
	}

	std::string base64_encode(unsigned char const* bytes_to_encode, uint32_t len);
	std::string base64_decode(std::string const& s);

	bool        ReadBool(   std::string& key, bool     defaultValue );
	int32_t     ReadInt(    std::string& key, int32_t   defaultValue );
	float       ReadReal(   std::string& key, float  defaultValue );
	std::string ReadString( std::string& key, char*    defaultValue );
	//
	// for use with constant sized data structures:
	uint8_t* ReadBinary( std::string& key, uint8_t* defaultValuePtr, uint32_t byte_size );

	char*    WriteString( std::string& key, char*    value );
	bool     WriteBool(   std::string& key, bool     value );
	int32_t  WriteInt(    std::string& key, int32_t   value );
	float    WriteReal(   std::string& key, float  value );
	//
	uint8_t* WriteBinary( std::string& key, uint8_t* valuePtr, uint32_t byte_size );
	
	// sync to persistent storage the contents of the key/value store; if terminal is false try to store to shared memory
	bool SyncToDiskStorage(bool doNotInit = false);				// attempt to sync to disk the contents of the key/value store

	int32_t ReadKeyValueStoreFromDisk(void); // read from disk the contents of the key/value store

	uint8_t* encrypt( uint8_t* msg, uint32_t msg_len, std::string const& key );
	uint8_t* decrypt( uint8_t* msg, uint32_t msg_len, std::string const& key );


	std::string		m_passkey;
	
	std::string		m_path;		// where config file is stored

	// -1 = created, uninitialized, 
	//  0 = created & okay, 
	//  1 = created, in error, library client not told, 
	//  2 = created, in error, library client told
	int32_t			m_state;			
	
	int32_t			m_writeBinaryErrorState;
	int32_t			m_readBinaryErrorState;

	std::map<std::string, CKeyValue> m_pairs;	// the key/value store itself is a std::map

	std::mutex	m_mutex;			// multi-threaded security

	// sqlite3 db fields:
	sqlite3*    mp_db;
	std::string m_db_fname;
	std::string m_emsg;
	
	bool				ExecuteSQL(sqlite3* db, const char* sql, std::string& emsg);
	int32_t			GetValFromDB(const char* sql);
	bool				CreateTables(void);
  bool				OpenDB(const char* fname);
	int32_t			SetValToDB(const CKeyValue& keyValue);
	int32_t			RemoveKeyFromDB(std::string& key);
};









#endif // _KVS_H_


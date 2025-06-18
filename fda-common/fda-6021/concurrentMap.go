package common

/*
	ConcurrentMap: Wraps a map in functions for thread safe access.

	Usage:
		This should be used when a map is accessed across mutiple goroutines.
		in an asynchronus manner.

		Additionally use this if values in the map will be changed or updated

		If there is a need for just writing values once. Consider using golangs
		sync.map: https://github.com/golang/go/blob/master/src/sync/map.go
*/
import (
	"sync"
)

/*
ConncurrentMap Interface takes 2 types a comparable type (string,int,bool...primatives)
and a type of any (this is the value type of the map)
*/
type ConcurrentMap[keyType comparable, valType any] interface {
	AddValue(key keyType, value valType) //Adds k\v pair To Map
	DoesKeyExist(key keyType) bool       //Verifies if map has a key
	RemovePair(key keyType) bool         //Removes a pair from the map. returns if process was successful
	makeMap()                            //Instatiates a new map with the types of (keyType,valType)
	GetValue(key keyType) *valType       //Returns the Value associated with the key
	Length() int                         //Returns the length of the map
}

/*
concurrentMap[keyType comparable, valType any]. Is a struct that implements
the CocurrentMap interface
*/
type concurrentMap[keyType comparable, valType any] struct {
	lock *sync.Mutex
	_map map[keyType]valType
}

/*
NewConncurrentMap: Returns a new ConcurrentMap
-access: public
- Takes 2 types.
 1. The type of our map key (must be a primative)
 2. The type of the map values (type can be any)

- Returns a Concurrent Map struct
*/
func NewConcurrentMap[keyType comparable, valType any]() ConcurrentMap[keyType, valType] {
	_map := &concurrentMap[keyType, valType]{}
	_map.makeMap()
	return _map
}

/*
makeMap: Initializes the map with the given types

-access:private

- Takes 2 types.
 1. The type of our map key (must be a primative)
 2. The type of the map values (type can be any)

- Returns nothing
*/
func (m *concurrentMap[keyType, valType]) makeMap() {
	if m.lock == nil {
		m.lock = &sync.Mutex{}
	}
	m.lock.Lock()
	defer m.lock.Unlock()

	if m._map == nil {
		m._map = make(map[keyType]valType)
	}
}

/*
GetValue: Reads Map for provided key, and retuns a value.

-access:public

- Takes 2 types.
 1. The type of our map key (must be a primative)
 2. The type of the map values (type can be any)

- Takes 1 parameter
	- Key of the specified type

- Returns a value of the specified type
*/

func (m *concurrentMap[keyType, valType]) GetValue(key keyType) *valType {

	var nilVal *valType
	if m != nil {
		m.lock.Lock()
		defer m.lock.Unlock()
		if value, ok := m._map[key]; ok {
			return &value
		}
	}
	return nilVal

}

/*
AddValue: Writes k/v pair to map.

-access:public

- Takes 2 types.
 1. The type of our map key (must be a primative)
 2. The type of the map values (type can be any)

- Takes 2 parameters
	- Key of the specified type
	- Value of the specified type

- Returns void
*/

func (m *concurrentMap[keyType, valType]) AddValue(key keyType, value valType) {
	if m != nil {
		m.lock.Lock()
		defer m.lock.Unlock()

		m._map[key] = value
	}

}

/*
DoesKeyExist: checks to see if the provided key exists in the map

-access:public

- Takes 1 parameter
  - Key of the specified type

- Returns bool
*/
func (m *concurrentMap[keyType, valType]) DoesKeyExist(key keyType) bool {
	if m._map != nil {
		m.lock.Lock()
		defer m.lock.Unlock()
		if _, ok := m._map[key]; ok {
			return true
		}
	}

	return false
}

/*
RemovePair: Removes a k/v pair from the map if key is found.

-access:public

- Takes 1 parameter
  - Key of the specified type

- Returns bool
*/
func (m *concurrentMap[keyType, valType]) RemovePair(key keyType) bool {

	if m._map != nil {
		m.lock.Lock()
		defer m.lock.Unlock()
		delete(m._map, key)
		return true
	}
	return false
}

/*
Length: Returns the current length of a map

-access:public

- Returns int
*/
func (m *concurrentMap[keyType, valType]) Length() int {
	return len(m._map)
}

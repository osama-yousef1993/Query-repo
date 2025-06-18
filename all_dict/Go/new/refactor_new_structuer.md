FOLDER Descriptions for the new Refactor Structure:

- App folder: in this folder we will defined all the functions that will return the response to UI or the End User.
- DataStruct folder : in this folder we will defined all struct and any other data type that wwe will use to map the data to it.
- Repository folder : in this folder we will defined all functions that will connect with any dataStore like (FS, PG, BQ) and return the data after we map it.
- Services folder: in this folder we will defined all business logic and return the response from it.

- in Repository and Services we need to defined an Interface and a Struct so we can connect all the function inside it in this case 

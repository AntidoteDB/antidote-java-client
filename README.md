All read methods: work in progress

Counters:

Write:

    public void updateCounter(String name, String bucket)
    Calls the other updateCounter method with value 1 as increment parameter

    public void updateCounter(String name, String bucket, Integer inc)
    Increments the counter with name name and bucket bucket by inc.

Read:

    public ApbGetCounterResp readCounter(String name, String bucket) 
    Gets you the Counter-Response

--------------------------------------------

Sets:

Write:

    public void removeSet(String name, String bucket, String element)
    Create a list containing one element and call the method below

    public void removeSet(String name, String bucket, List<String> elements)
    create a ApbSetUpdate.Builder and set a remove for each element of elements,
    Call updateSetHelper which contains code common for adding and removing elements

    public void addSet(String name, String bucket, String element)
    Create a list containing one element and call the method below

    public void addSet(String name, String bucket, List<String> elements)
    create a ApbSetUpdate.Builder and set an add for each element of elements,
    Call updateSetHelper which contains code common for adding and removing elements

    public void updateSetHelper(String name, String bucket, ApbSetUpdate.Builder setUpdateInstruction)
    Contains the code that can be used for both, adding and removing elements

Read:

    public ApbGetSetResp readSet(String name, String bucket)
    Gets you the Set-Response

--------------------------------------------

Registers:

Write:

    public void updateRegister(String name, String bucket, String value)
    Add a value to a LWW-Register. Like we did with sets, a helper method is used 

    public void updateMVRegister(String name, String bucket, String value)
    Add a value to a MV-Register. Like we did with sets, a helper method is used

    public void updateRegisterHelper(String name, String bucket, String value, ApbBoundObject.Builder regObject)
    Contains the code that can be used for both, registers and MV-registers

Read:

    public ApbGetRegResp readRegister(String name, String bucket)
    Gets you the LWW-Register-Response

    public ApbGetMVRegResp readMVRegister(String name, String bucket)
    Gets you the MV-Register-Response

--------------------------------------------

Integers:

Write:

    public void incrementInteger(String name, String bucket, Integer inc)
    Increment the Integer by inc.

    public void setInteger(String name, String bucket, Integer number)
    Set the integer to the value of parameter number.
    Call updateIntegerHelper

    public void updateIntegerHelper(String name, String bucket, ApbIntegerUpdate.Builder intUpdateInstruction)
    Contains the code that can be used for both, incrementing and setting an integer

Read:

    public ApbGetIntegerResp readInteger(String name, String bucket)
    Gets you the Integer-Response
    
--------------------------------------------
    
Maps:

Write:

    Three methods all preparing the parameters for a call of the method below, this approach is used to be more flexible with parameters
    public void updateMap(String name, String bucket, String key, CRDT_type type, ApbUpdateOperation update)
    public void updateMap(String name, String bucket, String key, CRDT_type type, List<ApbUpdateOperation> updates)
    public void updateMap(String name, String bucket, ApbMapKey key, ApbUpdateOperation update)
    
    public void updateMap(String name, String bucket, ApbMapKey mapKey, List<ApbUpdateOperation> updates)
    Takes a list of ApbUpdateOperation and an ApbMapKey, which contains the key string and the type of the entry.
    All updates in the list are executed on the map entry with the given key
    
    public void removeMap(String name, String bucket, ApbMapKey key)
    Create a list containing one ApbMapKey
    Call the method below with this list and the other parameters
    
    public void removeMap(String name, String bucket, List<ApbMapKey> keys)
    Remove the map entries with the given ApbMapKeys
    
Read:
    
    public void removeMap(String name, String bucket, List<ApbMapKey> keys)
    Gets you the AWMap-Response
    
--------------------------------------------
--------------------------------------------

Methods used to create ApbUpdateOperations needed for map updates
Pretty self-explanatory
    
    Counter:
    public ApbUpdateOperation createCounterIncrement()
    public ApbUpdateOperation createCounterIncrement(int value)
    Integer:
    public ApbUpdateOperation createIntegerIncrement(int value)
    public ApbUpdateOperation createIntegerSet(int value)
    Registry:
    public ApbUpdateOperation createRegSet(String value)
    Map:
    public ApbUpdateOperation createMapUpdate(String key, CRDT_type type, ApbUpdateOperation update)
    public ApbUpdateOperation createMapUpdate(ApbMapKey key, ApbUpdateOperation update)
    public ApbUpdateOperation createMapUpdate(String key, CRDT_type type, List <ApbUpdateOperation> updates)
    public ApbUpdateOperation createMapUpdate(ApbMapKey key, List <ApbUpdateOperation> updates)
    Set:
    public ApbUpdateOperation createSetAdd(String element)
    public ApbUpdateOperation createSetAdd(List<String> elements)
    public ApbUpdateOperation createSetRemove(String element)
    public ApbUpdateOperation createSetRemove(List<String> elements)
    
--------------------------------------------
--------------------------------------------

A getter for each CRDT that can be stored within a map
Pretty self-explanatory
Returns null if no such entry in the map
Used to simplify getting entries so no ApbMapKey must be built by hand

    public ApbGetCounterResp mapGetCounter(String name, String bucket, String key)
    public ApbGetIntegerResp mapGetInteger(String name, String bucket, String key)
    public ApbGetSetResp mapGetSet(String name, String bucket, String key)
    public ApbGetMVRegResp mapGetMVReg(String name, String bucket, String key)
    public ApbGetRegResp mapGetReg(String name, String bucket, String key)
    public ApbGetMapResp mapGetMap(String name, String bucket, String key)

    public ApbReadObjectResp getMapEntryHelper(String name, String bucket, String key, CRDT_type type)
    Helper used by all the above
    
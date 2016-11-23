All read methods: work in progress

Counters:

public void updateCounter(String name, String bucket)
Calls the other updateCounter method with value 1 as increment parameter

public void updateCounter(String name, String bucket, Integer inc)
Increments the counter with name name and bucket bucket by inc.

Sets:

public void removeSet(String name, String bucket, List<String> elements)
create a ApbSetUpdate.Builder called setUpdateInstruction and set a remove for each element of elements

public void removeSet(String name, String bucket, String element)
create a ApbSetUpdate.Builder called setUpdateInstruction and set a remove the parameter element

public void addSet(String name, String bucket, List<String> elements)
create a ApbSetUpdate.Builder called setUpdateInstruction and set an add for each element of elements

public void addSet(String name, String bucket, String element)
create a ApbSetUpdate.Builder called setUpdateInstruction and set an add the parameter element

public void updateSetHelper(String name, String bucket, ApbSetUpdate.Builder setUpdateInstruction)
Method called by all four preceding methods to gain some space

Registers:

public void updateRegister(String name, String bucket, String value)
Add a value to a register. Like we did with sets, a helper method is used 

public void updateMVRegister(String name, String bucket, String value)
Add a value to a MV-register. Like we did with sets, a helper method is used

public void updateRegisterHelper(String name, String bucket, String value, ApbBoundObject.Builder regObject)
Contains the code that can be used for both, registers and MV-registers

Integers:

public void incrementInteger(String name, String bucket, Integer inc)
Increment the Integer by inc. Like we did with sets, a helper method is used 

public void setInteger(String name, String bucket, Integer number)
Set the integer to the value of parameter number. Like we did with sets, a helper method is used 

public void updateIntegerHelper(String name, String bucket, ApbIntegerUpdate.Builder intUpdateInstruction)
Contains the common code of both above methods.

Maps:

public void updateMap(String name, String bucket, ApbMapKey key, ApbUpdateOperation update)
Generates a list of ApbUpdateOperation and calls the method below, which takes a list of ApbUpdateOperation as parameter

public void updateMap(String name, String bucket, ApbMapKey key, List<ApbUpdateOperation> updates)
Executes all the updates on the map element stored under key.

public void removeMap(String name, String bucket, ApbMapKey key)
Generates a list of ApbMapKey and calls the method below, which takes a list of ApbMapKey as parameter

public void removeMap(String name, String bucket, List<ApbMapKey> keys)
Removes all those elements from the map, whose keys are contained in parameter keys
import boto3
import os

region = os.environ['region']
instanceIp = os.environ['instanceIp']
instanceName = os.environ['instanceName']
date = os.environ['date']
imageId = os.environ['ami']
allVolumes = os.environ['allVolumes']
operation = os.environ['operation']
keyPair = os.environ['keyPair']
blockDevices = os.environ['blockDeviceList']
if blockDevices != '':
    blockDeviceList = blockDevices.split(",")
else:
    blockDeviceList = []

def describeInstancesByIp(region, instanceIp, instanceName):
    instanceDetails = ''
    ec2 = boto3.client('ec2', region_name=region)
    try:
        response = ec2.describe_instances(Filters=[{'Name': 'private-ip-address', 'Values': [instanceIp]},
                                                   {'Name': 'tag:Name', 'Values': [instanceName]}])
        if response['Reservations'] != [] and response['Reservations'][0]['Instances']:
            instanceDetails = response['Reservations'][0]['Instances'][0]
        else:
            print("No instance found with private Ip as: ", instanceIp, " and instance name as: ", instanceName)
    except Exception as e:
        print('Cannot fetch the details for instance: ', instanceIp)
        print(e)
    return instanceDetails

def getAllVolumes(instanceDetails):
    volumeDetailsList = []
    blockdeviceMappings = instanceDetails["BlockDeviceMappings"]
    if blockdeviceMappings != []:
        for device in blockdeviceMappings:
            deviceName = device["DeviceName"]
            volumeId = device.get('Ebs')['VolumeId']
            volumeDetailsList.append({'VolumeId': volumeId, 'DeviceName': deviceName})
    else:
        print("No volume attached to the instance", instanceDetails["InstanceId"])
    return volumeDetailsList

def getAvailabilityZone(instanceDetails):
    availabilityZone = instanceDetails.get("Placement")["AvailabilityZone"]
    return availabilityZone

def getTags(instanceDetails):
    tags = []
    if 'Tags' in instanceDetails.keys():
        tags = instanceDetails["Tags"]
    else:
        print("No tags found on ", instanceDetails["PrivateIpAddress"])
    return tags

def getRootVolumeDeviceName(instanceDetails):
    rootVolumeName = instanceDetails["RootDeviceName"]
    return rootVolumeName

def getRootVolumeId(volumeDetailsList, rootVolumeName):
    rootVolumeId = ''
    for volumeDetails in volumeDetailsList:
        if volumeDetails['DeviceName'] == rootVolumeName:
            rootVolumeId = volumeDetails['VolumeId']
    return rootVolumeId

def getInstanceId(instanceDetails):
    instanceId = instanceDetails["InstanceId"]
    return instanceId

def fetchSnapshotOfVolumeForDate(region, volumeId, date):
    snapshotList = []
    snapName = ''
    ec2 = boto3.client('ec2', region_name=region)
    try:
        response = ec2.describe_snapshots(Filters=[{'Name': 'volume-id', 'Values': [volumeId]}])

        if 'Snapshots' in response.keys():
            for snapshots in response["Snapshots"]:
                snapshotDate = snapshots["StartTime"].strftime('%d-%m-%Y')
                if snapshotDate == date:
                    snapDate = snapshots["StartTime"].strftime('%d%m%Y')
                    if 'Tags' in snapshots.keys():
                        for tag in snapshots["Tags"]:
                            if tag['Key'] == 'Name':
                                snapName = tag['Value']
                    snapshotList.append({'SnapshotId':snapshots["SnapshotId"], 'SnapshotName': snapName, 'SnapshotDate': snapDate})

            if snapshotList == []:
                print("No snapshpt found for volume ", volumeId, "for date ", date)
        else:
            print ("No snapshpt found for volume ", volumeId)
    except Exception as e:
        print("Error while fetching snapshpt for volume ", volumeId, "for date ", date)
        print(e)

    return snapshotList

def fetchVolumeDetails(region, volumeId):
    volumeDetails = {}
    ec2 = boto3.resource('ec2', region_name=region)
    try:
        volume = ec2.Volume(volumeId)
        size = volume.size
        encrypted = volume.encrypted
        kmsKey = volume.kms_key_id
        volumeType = volume.volume_type
        volumeDetails = {'VolumeId': volumeId, 'Size': size, 'Encrypted': encrypted, 'KmsKey': kmsKey,
                         'VolumeType': volumeType}
    except Exception as e:
        print ("Error while fetching volume details for ", volumeId)
        print (e)
    return volumeDetails

def createVolume(region, snapshotId, volumeSize, availabilityZone, volumeType, encrypted, kmsKey):
    volumeId = ''
    ec2 = boto3.client('ec2', region_name=region)
    try:
        if encrypted == False or kmsKey == None:
            encrypted = True
            kmsKey = 'alias/aws/ebs'
            #response = ec2.create_volume(SnapshotId=snapshotId, AvailabilityZone=availabilityZone,
             #                            VolumeType=volumeType, Size=volumeSize)
        #else:
        response = ec2.create_volume(SnapshotId=snapshotId, AvailabilityZone=availabilityZone, Encrypted=encrypted,
                                         KmsKeyId=kmsKey, VolumeType=volumeType, Size=volumeSize)
        volumeId = (response['VolumeId'])
    except Exception as e:
        print("Unable to create volume from ", snapshotId)
        print(e)
    return volumeId

def copyTags(region, tags, resourceId):
    ec2 = boto3.client('ec2', region_name=region)
    try:
        response = ec2.create_tags(Resources=[resourceId], Tags=tags)
    except Exception as e:
        print("Cannot copy tags to ", resourceId)
        print(e)

def stopInstance(region, instanceId):
    state = ''
    ec2 = boto3.resource('ec2', region_name=region)
    instance = ec2.Instance(instanceId)
    try:
        response = instance.stop(Force=True)
        instance.wait_until_stopped()
        state = instance.state['Name']
    except Exception as e:
        print("Cannot stop instance ", instanceId)
        print(e)
    return state

def terminateInstance(region, instanceId):
    state = ''
    ec2 = boto3.resource('ec2', region_name=region)
    instance = ec2.Instance(instanceId)
    try:
        response = instance.terminate()
        instance.wait_until_terminated()
        state = instance.state['Name']
    except Exception as e:
        print("Cannot stop instance ", instanceId)
        print(e)
    return state


def checkVolumeStatus(region, operation, resourceId):
    ec2 = boto3.client('ec2', region_name=region)
    try:
        waiter = ec2.get_waiter(operation)
        waiter.wait(VolumeIds=[resourceId])
        status = 'Completed'
    except Exception as e:
        status = "Operation " + operation + " not completed on resource " + resourceId + " in 600ms"
        print(e)
    return status

def detachVolume(region, instanceId, volumeId):
    ec2 = boto3.resource('ec2', region_name=region)
    instance = ec2.Instance(instanceId)
    try:
        response = instance.detach_volume(VolumeId=volumeId, Force=True)
    except Exception as e:
        print("Cannot detach volume ", volumeId, "from the instance ", instanceId)
        print(e)

def attachVolume(region, instanceId, volumeId, volumeName):
    ec2 = boto3.resource('ec2', region_name=region)
    instance = ec2.Instance(instanceId)
    try:
        response = instance.attach_volume(VolumeId=volumeId, Device=volumeName)
    except Exception as e:
        print("Cannot attach volume ", volumeId, "from the instance ", instanceId)
        print(e)

def startInstance(region, instanceId):
    state = ''
    ec2 = boto3.resource('ec2', region_name=region)
    instance = ec2.Instance(instanceId)
    try:
        response = instance.start()
        instance.wait_until_running()
        state = instance.state['Name']
    except Exception as e:
        print("Cannot start instance ", instanceId)
    return state


def identifyAutomaticSnapshot(snapshotList, tags, date):
    snapshotId = ''
    if tags == [] or date == '':
        print ("Cannot identify automatic snapshot")
        return snapshotId
    for tag in tags:
        if tag["Key"] == "Name":
            serverName = tag["Value"]

    snapDate = date.replace("-","")
    snapshotIdentity = "SS_"+serverName+"_"+snapDate
    print(snapshotIdentity)
    for snapshot in snapshotList:
        snapshotName = snapshot['SnapshotName']
        if snapshotIdentity in snapshotName:
            snapshotId = snapshot["SnapshotId"]
    return snapshotId


def replaceVolumes(region, instanceId, volumeId, volumeSize, volumeName, volumeType, encrypted, kmsKey, availabilityZone, tags, date):
    result = ''
    newVolumeId = ''
    print ("Fetching snapshot for ", volumeId , " for date ", date)
    snapshotList = fetchSnapshotOfVolumeForDate(region, volumeId, date)
    if snapshotList == []:
        print ("No snapshot found for volume ",volumeId,".")
        result = "No snapshot found for this volume."
        return result, ''
    if len(snapshotList) > 1:
        print ("More than one snapshots found for ", volumeId," for date ", date)
        print ("Identifying the automatic snapshots from these snapshots ", snapshotList)
        snapshotId = identifyAutomaticSnapshot(snapshotList, tags, date)
    if len(snapshotList) == 1:
        snapshotId = snapshotList[0]['SnapshotId']
    if snapshotId != '':
        print ("Creating New Volume from ", snapshotId)
        newVolumeId = createVolume(region, snapshotId, volumeSize, availabilityZone, volumeType,
                               encrypted, kmsKey)

        print("Checking availability status for newly created volume ", newVolumeId)
        status = checkVolumeStatus(region, 'volume_available', newVolumeId)
        if status == 'Completed':
            if tags != []:
                print("Copying tags from instance to the new volume ", newVolumeId)
                copyTags(region, tags, newVolumeId)
            else:
                print("No Tags found on ", instanceId)

            print ("Detaching existing volume ", volumeId)
            detachVolume(region, instanceId, volumeId)

            detachingstatus = checkVolumeStatus(region, "volume_available", volumeId)
            if detachingstatus == 'Completed':
                print ("Attaching new Volume ", newVolumeId)
                attachVolume(region, instanceId, newVolumeId, volumeName)
                attachingstatus = checkVolumeStatus(region, "volume_in_use", newVolumeId)
                if attachingstatus == 'Completed':
                    result = "Successfully Attached"
                else:
                    print ("Cannot attach volume ", newVolumeId)
                    result = attachingstatus
            else:
                print ("Can not dettach volume ", volumeId)
        else:
            print("Cannot create new volume from snapshot ", snapshotId)
    else:
        result = "No Snapshot found for volume "+volumeId
    return result, newVolumeId

def checkInstanceRunning(region, instanceId):
    state = ''
    ec2 = boto3.resource('ec2', region_name = region)
    try:
        instance = ec2.Instance(instanceId)
        instance.wait_until_running()
        state = instance.state['Name']
    except Exception as e:
        print ("Error while checking instance status")
        print (e)
    return state

def getVolumeIds(region, instanceId):
    volumeIds = []
    ec2 = boto3.resource('ec2', region_name = region)
    try:
        instance = ec2.Instance(instanceId)
        for volume in instance.volumes.all():
            volumeIds.append(volume.id)
    except Exception as e:
        print ("Error while fetching volumes attached to instance ", instanceId)
        print (e)
    return volumeIds

def createInstance(region, ami, instaceType, keyPair, securityGroupIds, subnetId,tags):
    instanceId = ''
    tagSpecification = [{'ResourceType': 'instance','Tags': tags}]
    networkInterfaces = [{'AssociatePublicIpAddress': True, 'Groups': securityGroupIds, 'SubnetId': subnetId, 'DeviceIndex':0}]
    ec2 = boto3.client('ec2', region_name = region)
    try:
        response = ec2.run_instances(
            ImageId=ami,
            InstanceType=instaceType,
            KeyName= keyPair,
            MaxCount=1,
            MinCount=1,
            NetworkInterfaces=networkInterfaces,
            TagSpecifications=tagSpecification,
        )
        instanceId = response['Instances'][0]['InstanceId']
    except Exception as e:
        print ("Failed to create a new instance")
        print (e)
    return instanceId

def createNewInstance(region, imageId, instanceDetails):
    securityGroupIds = []
    tags = getTags(instanceDetails)
    ami = imageId
    instanceType = instanceDetails['InstanceType']
    subnetId = instanceDetails['SubnetId']
    securityGroups = instanceDetails['SecurityGroups']
    for sg in securityGroups:
        securityGroupIds.append(sg['GroupId'])
    newInstanceId = createInstance(region, ami, instanceType, keyPair, securityGroupIds, subnetId, tags)

    runningState = checkInstanceRunning(region, newInstanceId)
    if runningState != 'running':
        newInstanceId = ''
    if tags !=[]:
        recoveryInstance = ''
        for tag in tags:
            if tag["Key"] == "Name":
                recoveryInstance = "Recovery_"+tag["Value"]
        newTag = [{"Key": "Name", "Value": recoveryInstance}]
        copyTags(region, newTag, newInstanceId)
    else:
        print ("No Tags found on original sever", instanceDetails["InstanceId"])
    return newInstanceId

def checkInstanceState(region, instanceId):
    state = ''
    ec2 = boto3.resource('ec2', region_name = region)
    try:
        instance = ec2.Instance(instanceId)
        state = instance.state['Name']
    except Exception as e:
        print ("Error checking the state of the instance ", instanceId)
        print (e)
    return state


def restoreAndAttachVolumes(region, instanceId, volumeId, volumeSize, volumeName,
                            volumeType, encrypted, kmsKey, availabilityZone, tags, date):
    result = ''
    newVolumeId = ''
    print("Fetching snapshot for ", volumeId, " for date ", date)
    snapshotList = fetchSnapshotOfVolumeForDate(region, volumeId, date)
    if snapshotList == []:
        print("No snapshot found for volume ", volumeId, ".")
        result = "No Snapshot found for this volume"
        return result, ''
    if len(snapshotList) > 1:
        print("More than one snapshots found for ", volumeId, " for date ", date)
        print("Identifying the automatic snapshots from these snapshots ", snapshotList)
        snapshotId = identifyAutomaticSnapshot(snapshotList, tags, date)
    else:
        snapshotId = snapshotList[0]['SnapshotId']
    if snapshotId != '':
        print("Creating New Volume from ", snapshotId)
        newVolumeId = createVolume(region, snapshotId, volumeSize, availabilityZone, volumeType,
                                   encrypted, kmsKey)

        print("Checking availability status for newly created volume ", newVolumeId)
        status = checkVolumeStatus(region, 'volume_available', newVolumeId)
        if status == 'Completed':
            if tags != []:
                print("Copying tags from instance to the new volume ", newVolumeId)
                copyTags(region, tags, newVolumeId)
            else:
                print("No Tags found on the original instance")

            print("Attaching new Volume ", newVolumeId)
            attachVolume(region, instanceId, newVolumeId, volumeName)
            attachingstatus = checkVolumeStatus(region, "volume_in_use", newVolumeId)
            if attachingstatus == 'Completed':
                result = "Successfully Attached"
            else:
                print("Cannot attach volume ", newVolumeId)
                result = attachingstatus
        else:
            print("Cannot create new volume from snapshot ", snapshotId)
    else:
        result = "No Snapshot found for volume "+volumeId
    return result, newVolumeId


def replace(instanceDetails, allVolumes, blockDeviceList):
    replaceResult = []
    msg = ''
    print("Getting availability zone of the instance : ", instanceIp)
    availabilityZone = getAvailabilityZone(instanceDetails)

    print("Getting tags attached to the instance : ", instanceIp)
    tags = getTags(instanceDetails)

    print("Getting instance Id of the instance : ", instanceIp)
    instanceId = getInstanceId(instanceDetails)

    print("Getting all volumes attached to the instance : ", instanceIp)
    volumeDetailsList = getAllVolumes(instanceDetails)

    if volumeDetailsList != []:
        print("Getting current state of the instance ", instanceId)
        currentState = checkInstanceState(region, instanceId)
        if currentState != 'stopped':
            print("Stopping instance ", instanceId)
            currentState = stopInstance(region, instanceId)
        if currentState == 'stopped':
            if allVolumes == "Yes":
                print("Replacing all volumes for instance : ", instanceIp)
                for volume in volumeDetailsList:
                    volumeId = volume['VolumeId']
                    volumeName = volume['DeviceName']

                    print("Proceesing for volume  : ", volumeId)
                    volumeDetails = fetchVolumeDetails(region, volumeId)
                    if volumeDetails != {}:
                        volumeSize = volumeDetails['Size']
                        volumeType = volumeDetails['VolumeType']
                        encrypted = volumeDetails['Encrypted']
                        kmsKey = volumeDetails['KmsKey']
                        replaceStatus, newVolumeId = replaceVolumes(region, instanceId, volumeId, volumeSize, volumeName, volumeType,
                                                       encrypted, kmsKey, availabilityZone, tags, date)

                        replaceResult.append({'OriginalVolumeId': volumeId, 'NewVolumeId': newVolumeId, 'ReplaceStatus': replaceStatus})
                    else:
                        print ("Cannot fetch the details for ", volumeId)
            else:
                for volume in volumeDetailsList:
                    volumeId = volume['VolumeId']
                    volumeName = volume['DeviceName']
                    for blockdevice in blockDeviceList:
                        volumeFound = False
                        if volumeName==blockdevice:
                            volumeFound = True
                        if volumeFound:
                            print("Proceesing for volume  : ", volumeId, "with block device name as ", volumeName)
                            volumeDetails = fetchVolumeDetails(region, volumeId)
                            if volumeDetails != {}:
                                volumeSize = volumeDetails['Size']
                                volumeType = volumeDetails['VolumeType']
                                encrypted = volumeDetails['Encrypted']
                                kmsKey = volumeDetails['KmsKey']
                                replaceStatus, newVolumeId = replaceVolumes(region, instanceId, volumeId, volumeSize,
                                                                            volumeName, volumeType,
                                                                            encrypted, kmsKey, availabilityZone, tags,
                                                                            date)

                                replaceResult.append({'OriginalVolumeId': volumeId, 'NewVolumeId': newVolumeId,
                                                      'ReplaceStatus': replaceStatus})
                            else:
                                print("Cannot fetch the details for ", volumeId)
                        else:
                            print ("Not processing volume ", volumeId)

            print("Starting instance ", instanceId)
            startState = startInstance(region, instanceId)
            if startState == 'running':
                print("Volume/(s) replaced successfully")
                msg = 'Operation Completed Successfully.'
            else:
                print("Cannot start Instance ", instanceId)
                msg = 'Cannot start the instance after attaching the volume'
        else:
            print("Cannot stop instance ", instanceId)
    else:
        print("No volume attached to instance ", instanceIp)
        msg = "No recovery operation performed."
        replaceResult = "No Volume attached to the original server."


    result = {"OriginalInstance": instanceIp, "OriginalInstanceId": instanceId,
              "Operation": operation, "VolumeRestoreStatus": replaceResult, "Result": msg}
    return result

def recreate(instanceDetails, allVolumes, blockDeviceList, imageId):
    msg = ''
    recreateResult = []
    print("Creating New Instance")
    newInstanceId = createNewInstance(region, imageId, instanceDetails)
    if newInstanceId == '':
        print("Instance did not launch successfully. Exiting")
        result = {"Result": "Problem launching new recovery Instance."}
        return result
    print ("Stopping the newly created instance ", newInstanceId)
    stopState = stopInstance(region, newInstanceId)
    if stopState == 'stopped':
        print ("Fetching all volumes attached to the new instance ", newInstanceId)
        volIdList = getVolumeIds(region, newInstanceId)

        print("Detaching existing volume from the new instance")
        for volId in volIdList:
            detachVolume(region, newInstanceId, volId)
            detachingstatus = checkVolumeStatus(region, "volume_available", volId)
            if detachingstatus != 'Completed':
                print("Can not dettach volume ", volId, " from new recovery instance ", newInstanceId)
                result = {"Result": "Problem detaching existing volume from the recovery server."}
                return result
    else:
        print("Cannot stop new recovery instance ", newInstanceId)
        result = "Problem occured while stopping the new recovery server"
        return result

    print("Getting availability zone of the instance : ", instanceIp)
    availabilityZone = getAvailabilityZone(instanceDetails)

    print("Getting tags attached to the instance : ", instanceIp)
    tags = getTags(instanceDetails)

    print("Getting instance Id of the instance : ", instanceIp)
    instanceId = getInstanceId(instanceDetails)

    print("Getting all volumes attached to the oiginal instance : ", instanceIp)
    volumeDetailsList = getAllVolumes(instanceDetails)
    if volumeDetailsList != []:
        if allVolumes == "Yes":
            for volume in volumeDetailsList:
                volumeId = volume['VolumeId']
                volumeName = volume['DeviceName']

                print("Getting Details for volume ", volumeId)
                volumeDetails = fetchVolumeDetails(region, volumeId)
                if volumeDetails != {}:
                    volumeSize = volumeDetails['Size']
                    volumeType = volumeDetails['VolumeType']
                    encrypted = volumeDetails['Encrypted']
                    kmsKey = volumeDetails['KmsKey']

                    restoreStatus, newVolumeId = restoreAndAttachVolumes(region, newInstanceId, volumeId, volumeSize,volumeName, volumeType, encrypted, kmsKey, availabilityZone, tags, date)

                    recreateResult.append({'OriginalVolumeId': volumeId, 'NewVolumeId': newVolumeId,
                                          'VolumeStatus': restoreStatus})
                else:
                    print("Cannot fetch the details for ", volumeId)
        else:
            for volume in volumeDetailsList:
                volumeId = volume['VolumeId']
                volumeName = volume['DeviceName']
                for blockdevice in blockDeviceList:
                    volumeFound = False
                    if volumeName == blockdevice:
                        volumeFound = True
                    if volumeFound:
                        print("Proceesing for volume  : ", volumeId, "with block device name as ", volumeName)
                        volumeDetails = fetchVolumeDetails(region, volumeId)
                        if volumeDetails != {}:
                            volumeSize = volumeDetails['Size']
                            volumeType = volumeDetails['VolumeType']
                            encrypted = volumeDetails['Encrypted']
                            kmsKey = volumeDetails['KmsKey']
                            restoreStatus, newVolumeId = restoreAndAttachVolumes(region, newInstanceId, volumeId,
                                                                                 volumeSize, volumeName, volumeType,
                                                                                 encrypted, kmsKey, availabilityZone,
                                                                                 tags, date)

                        else:
                            print ("The blockdevice ", blockdevice, "does not exist on the instance", instanceId)
                            restoreStatus = "The blockdevice "+ blockdevice+ "does not exist on the instance"+ instanceId
                        recreateResult.append({'OriginalVolumeId': volumeId, 'NewVolumeId': newVolumeId,
                                               'VolumeStatus': restoreStatus})
        print("Starting instance ", newInstanceId)
        startState = startInstance(region, newInstanceId)
        if startState == 'running':
            print("Volume/(s) replaced successfully")
            msg = "Operation Completed Successfully"
        else:
            print("Cannot start Instance ", newInstanceId)
            msg = "Cannot start the recovery instance"

    else:
        print("No volume attached to instance ", instanceIp)
        print ("Terminating the newly created instance: ", newInstanceId," as no operation is performed on it." )
        recreateResult = "No Volume attached to the original Server"
        terminatedState = terminateInstance(region, newInstanceId)
        if terminatedState != 'terminated':
            print ("Error while terminating the new recovery instance ", newInstanceId)
            msg = "No restore activity performed. Error while terminating the new recovery instance "
        else:
            msg = "No restore activity performed. The new recovery instance was terminated"


    result = {"OriginalInstance": instanceIp, "OriginalInstanceId": instanceId, "NewInstanceId": newInstanceId,
              "Operation": operation, "VolumeRestoreStatus": recreateResult, "Result": msg}

    return result

def mainFunc(operation):
    if blockDeviceList == [] and allVolumes == 'No':
        print ("No block devices provided as input. Aborting operation")
        result = ("No operation performed. Either select all volumes to be restored or provide the block device/(s) names to be restored")
        return result
    print("Fetching details for the original instance : ", instanceIp)
    instanceDetails = describeInstancesByIp(region, instanceIp, instanceName)
    if instanceDetails != '':
        print("Starting ", operation, " Operation.")
        if operation == 'replace':
            result = replace(instanceDetails, allVolumes, blockDeviceList)
        else:
            result = recreate(instanceDetails, allVolumes, blockDeviceList, imageId)
        print ("Operation Completed Successfully.")
    else:
        print("Instance not found with ip ", instanceIp, " and name as ", instanceName)
        msg = "Instance not found with ip "+ instanceIp+ " and name as "+instanceName
        result = {"Result": msg}
    return result
if __name__ == '__main__':
    result = mainFunc(operation)
    print ("Final Result is: ")
    print (result)

# This script is for a dirty DR solution for snapshots. This script copies the snapshot from the source region to the target region (cross region) usin a custom KMS. Then shares the copied snapshot from one AWS account to another AWS account(cross Account) using default kms

import boto3
import botocore
import sys
    
def assumeRole(roleArn):
	response = ''
	sts = boto3.client('sts')
	try:
		response = sts.assume_role(RoleArn=roleArn, RoleSessionName='share-admin-temp-session')
	except Exception as e:
		print (e)
	credentials = response['Credentials']
	return credentials   
    
def share_snapshot(targetAccountid, snapshot_id, region):
	
	roleArn = 'arn:aws:iam::'+sourceAccountId+':role/'+roleName
	credentials = assumeRole(roleArn)

	ec2 = boto3.client('ec2', region_name=region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
	
	print("Sharing With Target Account....")
	
	# Sharing with target account
	try:
		out = ec2.modify_snapshot_attribute(Attribute='createVolumePermission', OperationType='add', SnapshotId=snapshot_id, UserIds=[targetAccountid])
		print("shared snapshot with target account")
	except Exception as e:
		print (e.message)
    
def checkStatusOfSnapshot(snapshot_id, region, accountId):
    
	roleArn = 'arn:aws:iam::'+accountId+':role/'+roleName
	credentials = assumeRole(roleArn)
        
	ec2 = boto3.client('ec2', region_name=region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
        
	print ("id:",snapshot_id)
	output = ec2.describe_snapshots(SnapshotIds=[snapshot_id])
	print (output)
			
	try:
		waiter = ec2.get_waiter('snapshot_completed')
		waiter.wait(SnapshotIds=[snapshot_id])
	except botocore.exceptions.WaiterError as e:
		print("Snapshot not copied in 600 ms: ", snapshot_id)
		status = "Snapshot not shared in 600 ms: " + snapshot_id
		print (e)
	print ("Snapshot copy operation status is now 'Completed' ")
	status = 'Completed'
	return status
    
def copy_snapshot(snapshot_id, source_region, destination_region, stagingKey, accountId, tags, description):
	roleArn = 'arn:aws:iam::'+accountId+':role/'+roleName
	credentials = assumeRole(roleArn)
        
	ec2 = boto3.resource('ec2', region_name=destination_region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
	snapshot = ec2.Snapshot(snapshot_id)
	res = ''
	print ("Started copying.. snapshot_id: " , snapshot_id , ", ```````````from: " , source_region , ", to: " , destination_region)
	if stagingKey!='' :
		res = snapshot.copy(SourceRegion=source_region, DestinationRegion=destination_region, Description=description, KmsKeyId = stagingKey, Encrypted = True, TagSpecifications = [{'ResourceType': 'snapshot', 'Tags': tags }])
	else:
		res = snapshot.copy(SourceRegion=source_region, DestinationRegion=destination_region, Description=description, Encrypted = True, TagSpecifications = [{'ResourceType': 'snapshot', 'Tags': tags }])
	print (res)
	copiedSnapshotId=res['SnapshotId']
	if res["ResponseMetadata"]["HTTPStatusCode"] == 200:
		print ("Successfully copyied.. snapshot_id: " , snapshot_id , ", from: " , source_region , ", to: " , destination_region)
	else:
		print ("Failed to copy.. snapshot_id: " , snapshot_id , ", from: " , source_region , ", to: " , destination_region)
	return copiedSnapshotId
    
def sortSnapshots(snapshotList):
	latestSnapshot = sorted(snapshotList, key=lambda k: k['startTime'], reverse=True)[0]
	#latestSnapshotId = latestSnapshot["SnapshotId"]
	return latestSnapshot
        
def getAllSnapshot(region, volumeId, sourceAccountId):
	roleArn = 'arn:aws:iam::'+sourceAccountId+':role/'+roleName
	credentials = assumeRole(roleArn)
        
	snapshotList = []
	ec2 = boto3.client('ec2', region_name = region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
	try:
		response = ec2.describe_snapshots(OwnerIds=[sourceAccountId], Filters=[{'Name': 'volume-id', 'Values':[volumeId]}])
		for snapshot in response['Snapshots']:
			snapshotId = snapshot['SnapshotId']
			startTime = snapshot['StartTime']
			tags =[]
			if 'Tags' in snapshot.keys():
				tags = snapshot['Tags']
			else:
				print "No tags available on the source snapshot: ", snapshotId
			snapshotList.append({'SnapshotId':snapshotId, 'startTime': startTime, 'Tags':tags})
	except botocore.exceptions.ClientError as e:
		print ("error while fetching snapshot")
	return snapshotList
      
def getVolumeList(instance):
	volumeList = []
	blockDeviceMappings = instance["BlockDeviceMappings"]
	if blockDeviceMappings != []:
		for ebs in blockDeviceMappings:
			deviceName = ebs["DeviceName"]
			volumeId = ebs.get("Ebs")["VolumeId"]
			volumeList.append({'VolumeId':volumeId, 'DeviceName':deviceName})
			print (volumeId)
			print (deviceName)
	else:
		print("No volume is attached to the instance: ", instance["InstanceId"])
        
	return volumeList
    
def getAllInstances(region):
	roleArn = 'arn:aws:iam::'+sourceAccountId+':role/'+roleName
	credentials = assumeRole(roleArn)
        
	ec2 = boto3.client('ec2', region_name=region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
	next_token = ''
	instanceList = ''
	while next_token is not None:
		try:
			response = ec2.describe_instances(Filters=[{'Name': 'tag:ClarityID', 'Values':[clarityId]}, {'Name': 'tag:CostCenter', 'Values':[costCenter]}])
			print (response)
			if response['Reservations'] != []:
				instanceList = response['Reservations'][0]['Instances']
                       
			else:
				print ("No instance has the Clarity Id and Cost Center combination provides")
			next_token = response.get('NextToken')
		except Exception as e:
			print (e)
                
	return instanceList
    
def addTags(accountId, region, tags, resourceId):
	roleArn = 'arn:aws:iam::'+accountId+':role/'+roleName
	credentials = assumeRole(roleArn)
        
	ec2 = boto3.client('ec2', region_name=region, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
	try:
		response = ec2.create_tags(Resources=[resourceId], Tags=tags)
	except Exception as e:
		print("Error while adding tags to resource: ", resourceId)
		print (e.message)

def mainFunc():
	print ("Fetching All Instances with the given Clarity Id and Cost Center Id")
	instanceList = getAllInstances(region)
	if instanceList != []:
		for instance in instanceList:
			print ("Get List of Volumes attached with the Instance")
			volumeList = getVolumeList(instance)
			if volumeList != []:
				for volume in volumeList:
					volumeId = volume['VolumeId']
					deviceName = volume['DeviceName']
					tags = []
					print ("Get all snapshots created from the given volune")
					snapshotList = getAllSnapshot(region, volumeId, sourceAccountId)
					if snapshotList != []:
						print ("Get the latest snapshot Id")
						latestSnapshot = sortSnapshots(snapshotList)
						latestSnapshotId = latestSnapshot["SnapshotId"]
						tags = latestSnapshot["Tags"]
						description = 'DRSnapshot created for (root/additional) volume '+ volumeId+'(device'+deviceName +') of instance '+instance["InstanceId"]
						print (description)
						
						if tags == []:
							msg = "No Tags Available On Source Snapshot "+latestSnapshotId
							print (msg)
							tags = [{'Key':'TagValues', 'Value': msg}]
                
						print ("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$ ", tags)
						print ("Copy Snapshot to in the same  region")
						copiedSnapshotIdSameRegion = copy_snapshot(latestSnapshotId, region, region, stagingKey, sourceAccountId, tags, description)
                                    
						print("Checking status of copy operation")
						statusSameRegion = checkStatusOfSnapshot(copiedSnapshotIdSameRegion, region, sourceAccountId)
                            
						print ("Copied snapshot id: ", copiedSnapshotIdSameRegion)
                            
						if statusSameRegion == 'Completed':
							print("sharing the snapshot post copying in same region")	
                                
							share_snapshot(targetAccountId, copiedSnapshotIdSameRegion, region)
							statusSharingSnapshot=checkStatusOfSnapshot(copiedSnapshotIdSameRegion, region,targetAccountId)
            
							if 	statusSharingSnapshot == 'Completed':
								print ("Adding tags to the shared snapshot")
								addTags(targetAccountId, region, tags, copiedSnapshotIdSameRegion)
								print ("Copy Snapshot  in the target  region of target account")
								copiedSnapshotIdTargetRegion = copy_snapshot(copiedSnapshotIdSameRegion, region, targetRegion, '',targetAccountId, tags, description)
                                    
								print("Checking status of copy operation")
								statusTargetRegion = checkStatusOfSnapshot(copiedSnapshotIdTargetRegion, targetRegion,targetAccountId)
                                    
								print("Checking status of copy operation")
                                
								if statusTargetRegion == 'Completed':
									print("snapshot shared having id ", copiedSnapshotIdTargetRegion)
								else:
									print(statusTargetRegion)
							else:
								print(statusSharingSnapshot)
						else:
							print(statusSameRegion)
                
                            
					else:
						print ("No Snapshot available for volume: ", volumeId)
			else:
				print ("No Volume attached to the Instance : ", instance["InstanceId"])
	else:
		print ("Clarity Id or Cost Center does not match the input provided for any of the instances")
        #print (ec2Details)
    
    ##################### Calling main Function ##########################
    
    
sourceAccountId = str(sys.argv[1])
targetAccountId = str(sys.argv[2])
region = str(sys.argv[3])
targetRegion = str(sys.argv[4])
roleName = str(sys.argv[5])
#targetRoleName = 'replicatesnapshot'
clarityId = str(sys.argv[6])
costCenter = str(sys.argv[7])
key = str(sys.argv[8])
stagingKey = 'alias/' + key
    
print (" Start DR Operation ")
mainFunc()

print ("DR Operation Completed ")

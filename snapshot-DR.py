# This script is for a dirty DR solution for snapshots. This script copies the snapshot from the source region to the target region (cross region) usin a custom KMS. Then shares the copied snapshot from one AWS account to another AWS account(cross Account) using default kms

import boto3
import botocore

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
    
    #roleArn = 'arn:aws:iam::'+sourceAccountId+':role/'+roleName
    #credentials = assumeRole(roleArn)
    
    ec2 = boto3.client('ec2', region_name=region)#, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
    
    print("Sharing With Target Account....")
    
    # Sharing with target account
    try:
        out = ec2.modify_snapshot_attribute(Attribute='createVolumePermission', OperationType='add', SnapshotId=snapshot_id, UserIds=[targetAccountid])
        print("shared snapshot with target account")
    except Exception as e:
        print (e.message)

def checkStatusOfSnapshot(snapshot_id, region):

    #roleArn = 'arn:aws:iam::'+accountId+':role/'+roleName
    #credentials = assumeRole(roleArn)
    
    ec2 = boto3.client('ec2', region_name=region)#, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
    
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

def fetch_allSnapshots(accountId, region):
    #roleArn = 'arn:aws:iam::'+accountId+':role/'+rolename
    #credentials = assumeRole(roleArn)
    
    ec2 = boto3.client('ec2', region_name = region)#, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
    
    try:
        response = ec2.describe_snapshots(OwnerIds=[sourceAccountId])
        
    except Exception as e:
        print (e.message)
    return response

def copy_snapshot(snapshot_id, source_region, destination_region, stagingKey):
    #roleArn = 'arn:aws:iam::'+accountId+':role/'+roleName
    #credentials = assumeRole(roleArn)
    
    ec2 = boto3.resource('ec2', region_name=destination_region)#, aws_access_key_id=credentials['AccessKeyId'], aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken'])
    snapshot = ec2.Snapshot(snapshot_id)
    print ("Started copying.. snapshot_id: " + snapshot_id + ", ```````````from: " + source_region + ", to: " + destination_region)
    res = snapshot.copy(SourceRegion=source_region, DestinationRegion='eu-central-1', Description='Automatic Copy', KmsKeyId = 'alias/StageKey', Encrypted = True)
    print (res)
    copiedSnapshotId=res['SnapshotId']
    if res["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print ("Successfully copyied.. snapshot_id: " + snapshot_id + ", from: " + source_region + ", to: " + destination_region)
    else:
        print ("Failed to copy.. snapshot_id: " + snapshot_id + ", from: " + source_region + ", to: " + destination_region)
    return copiedSnapshotId


def mainFunc():
    print ('Fetching all snapshots')
    response = fetch_allSnapshots(sourceAccountId, sourceRegion)
    for snapshot in response['Snapshots']:
        snapshotId = snapshot['SnapshotId']
        print ("Processing snapshot: ",snapshotId)
        
        print ("Fetching cost center and clarity Id")
        cid = ''
        cc = ''
    
        if 'Tags' in snapshot.keys():  
            for tag in snapshot['Tags']:
                
                if tag['Key'] == 'Tag1':
                    cid = tag['Value']
                    
                if tag['Key'] == "Tag2":
                    cc = tag['Value']
                    
        else:
            print ('No tags defined on ',snapshotId)
            break
                           
        if cid == tag1 and cc == tag2:
            print ("Copy Snapshot to target region")
            copiedSnapshotId = copy_snapshot(snapshotId, sourceRegion, targetRegion, stagingKey)
            print ("Copied snapshot id: ", copiedSnapshotId)
            print("Checking status of copy operation")
            status = checkStatusOfSnapshot(copiedSnapshotId, targetRegion)
            
            if status == 'Completed':
                share_snapshot(targetAccountId, copiedSnapshotId, targetRegion)
            else:
                print(status)
        else:
            print ("Clarity Id or Cost Center does not match the input provided for: ", snapshotId)
    
    
###################### Calling main Function ##########################


sourceAccountId = '111111111111'
targetAccountId = '999999999999'
sourceRegion = 'eu-west-1'
targetRegion = 'eu-central-1'
#sourceRoleName = 'ReplicateSnapshot'
#targetRoleName = 'replicatesnapshot'
tag1 = 'cid2'
tag2 = 'cc2'

print (" Start DR Operation ")
mainFunc()

print ("DR Operation Completed ")

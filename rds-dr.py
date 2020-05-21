import boto3
import botocore
import sys
import datetime


def assumeRole(roleArn):
    response = ''
    sts = boto3.client('sts')
    try:
        response = sts.assume_role(RoleArn=roleArn, RoleSessionName='share-admin-temp-session')
    except Exception as e:
        print(e)
    credentials = response['Credentials']
    return credentials


def shareSnapshot(sourceAccountId, targetAccountId, region, snapshot_id):
    roleArn = 'arn:aws:iam::' + sourceAccountId + ':role/' + roleName
    credentials = assumeRole(roleArn)

    rds = boto3.client('rds', region_name=region, aws_access_key_id=credentials['AccessKeyId'],
                       aws_secret_access_key=credentials['SecretAccessKey'],
                       aws_session_token=credentials['SessionToken'])

    sharedSnapshotId = ''
    print("Sharing snapshot ", snapshot_id, "from ", sourceAccountId, " to ", targetAccountId)

    # Sharing with target account
    try:
        out = rds.modify_db_snapshot_attribute(AttributeName='restore', DBSnapshotIdentifier=snapshot_id,
                                               ValuesToAdd=[targetAccountId])
        print("shared snapshot with target account")
        print("response of share operation: ", out)
        sharedSnapshotId = "arn:aws:rds:" + region + ":" + sourceAccountId + ":snapshot:" + snapshot_id
    except Exception as e:
        print(e.message)

    return sharedSnapshotId


def checkStatusOfSnapshot(snapshot_id, region, accountId):
    # Get session with target account
    roleArn = 'arn:aws:iam::' + accountId + ':role/' + roleName
    credentials = assumeRole(roleArn)

    rds = boto3.client('rds', region_name=region, aws_access_key_id=credentials['AccessKeyId'],
                       aws_secret_access_key=credentials['SecretAccessKey'],
                       aws_session_token=credentials['SessionToken'])

    print("id:", snapshot_id)
    output = rds.describe_db_snapshots(DBSnapshotIdentifier=snapshot_id)
    # print (output)

    try:
        waiter = rds.get_waiter('db_snapshot_completed')
        waiter.wait(DBSnapshotIdentifier=snapshot_id)
    except botocore.exceptions.WaiterError as e:
        print("Snapshot not copied in 600 ms: ", snapshot_id)
        status = "Snapshot not shared in 600 ms: " + snapshot_id
        print(e)
    print("Snapshot copy/share operation status is now 'Completed' ")
    status = 'Completed'
    return status


def copySnapshot(accountId, sourceRegion, targetRegion, snapshotId, stagingKey, dbId, copyTagFlag):
    roleArn = 'arn:aws:iam::' + accountId + ':role/' + roleName
    credentials = assumeRole(roleArn)
    copiedSnapshotId = ''
    now = datetime.datetime.now()
    currentTime = now.strftime("%Y-%m-%d-%H-%M-%S")
    rds = boto3.client('rds', region_name=targetRegion, aws_access_key_id=credentials['AccessKeyId'],
                       aws_secret_access_key=credentials['SecretAccessKey'],
                       aws_session_token=credentials['SessionToken'])

    print("Started copying.. snapshot_id: ", snapshotId, ", from: ", sourceRegion, ", to: ", targetRegion)
    targetDBSnapshotId = dbId + "-" + currentTime
    try:
        res = ''
        if stagingKey != '':
            res = rds.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshotId,
                                       TargetDBSnapshotIdentifier=targetDBSnapshotId, KmsKeyId=stagingKey,
                                       CopyTags=copyTagFlag, SourceRegion=sourceRegion)
        else:
            res = rds.copy_db_snapshot(SourceDBSnapshotIdentifier=snapshotId,
                                       TargetDBSnapshotIdentifier=targetDBSnapshotId, KmsKeyId='alias/aws/rds',
                                       CopyTags=copyTagFlag, SourceRegion=sourceRegion)
    except Exception as e:
        print("Error while performing Copy OPeration")
        print(e.message)

    # print (res)
    copiedSnapshotId = res.get('DBSnapshot')['DBSnapshotIdentifier']

    if res["ResponseMetadata"]["HTTPStatusCode"] == 200:
        print("Successfully copyied.. snapshot_id: ", snapshotId, ", from: ", sourceRegion, ", to: ", targetRegion)
    else:
        print("Failed to copy.. snapshot_id: ", snapshotId, ", from: ", sourceRegion, ", to: ", targetRegion)
    return copiedSnapshotId


def getAllDbInstances(accountId, region):
    roleArn = 'arn:aws:iam::' + accountId + ':role/' + roleName
    credentials = assumeRole(roleArn)

    rds = boto3.client('rds', region_name=region, aws_access_key_id=credentials['AccessKeyId'],
                       aws_secret_access_key=credentials['SecretAccessKey'],
                       aws_session_token=credentials['SessionToken'])
    response = ''
    dbInstanceList = []
    try:
        response = rds.describe_db_instances()
        dbInstanceList = response['DBInstances']
    except Exception as e:
        print("Error encountered while fetching all db instances")
    return dbInstanceList


def getAllTags(accountId, region, resourceArn):
    roleArn = 'arn:aws:iam::' + accountId + ':role/' + roleName
    credentials = assumeRole(roleArn)

    rds = boto3.client('rds', region_name=region, aws_access_key_id=credentials['AccessKeyId'],
                       aws_secret_access_key=credentials['SecretAccessKey'],
                       aws_session_token=credentials['SessionToken'])
    tagList = []
    try:
        response = rds.list_tags_for_resource(ResourceName=resourceArn)
        tagList = response['TagList']
    except Exception as e:
        print("Error encountered while fetching all db instances")
    return tagList


def checkTags(tagList, clarityId, costCenter):
    cid = ''
    cc = ''
    isInstanceEligible = False
    for tag in tagList:
        if tag['Key'] == 'ClarityID':
            cid = tag['Value']
        if tag['Key'] == "CostCenter":
            cc = tag['Value']

    if cid == clarityId and cc == costCenter:
        isInstanceEligible = True
    return isInstanceEligible


def getSnapshotList(accountId, region, dbId):
    roleArn = 'arn:aws:iam::' + accountId + ':role/' + roleName
    credentials = assumeRole(roleArn)

    rds = boto3.client('rds', region_name=region, aws_access_key_id=credentials['AccessKeyId'],
                       aws_secret_access_key=credentials['SecretAccessKey'],
                       aws_session_token=credentials['SessionToken'])
    snapshotList = []
    try:
        response = rds.describe_db_snapshots(DBInstanceIdentifier=dbId)
        for snapshot in response['DBSnapshots']:
            snapshotId = snapshot['DBSnapshotIdentifier']
            startTime = snapshot['SnapshotCreateTime']
            dbSnapshotArn = snapshot['DBSnapshotArn']
            snapshotList.append({'SnapshotId': snapshotId, 'startTime': startTime, 'DBSnapshotArn':dbSnapshotArn})
    except Exception as e:
        print("Error encountered while fetching all db snapshots")
    print("snapshotList: ", snapshotList)
    return snapshotList


def sortSnapshots(snapshotList):
    latestSnapshot = sorted(snapshotList, key=lambda k: k['startTime'], reverse=True)[0]
    # latestSnapshotId = latestSnapshot["SnapshotId"]
    return latestSnapshot


def addTags(accountId, region, tags, resourceId):
    roleArn = 'arn:aws:iam::' + accountId + ':role/' + roleName
    credentials = assumeRole(roleArn)

    rds = boto3.client('rds', region_name=region, aws_access_key_id=credentials['AccessKeyId'],
                       aws_secret_access_key=credentials['SecretAccessKey'],
                       aws_session_token=credentials['SessionToken'])
    try:
        response = rds.add_tags_to_resource(ResourceName=resourceId, Tags=tags)
    except Exception as e:
        print("Error while adding tags to resource: ", resourceId)
        print(e.message)


def getTags(accountId, region, snapshotArn):
    roleArn = 'arn:aws:iam::' + accountId + ':role/' + roleName
    credentials = assumeRole(roleArn)


    tagListParticularResource = []
    rds = boto3.client('rds', region_name=region, aws_access_key_id=credentials['AccessKeyId'],
                   aws_secret_access_key=credentials['SecretAccessKey'], aws_session_token=credentials['SessionToken']) 
    try:
        response = rds.list_tags_for_resource(ResourceName=snapshotArn)
        tagListParticularResource = response['TagList']
    except Exception as e:
        print("Error while gettimg tags", snapshotArn)
        print(e.message)
    print("Tags for the particluar resource :", tagListParticularResource)
    return tagListParticularResource


def mainFunc():
    print("Fetching all db instance in account", sourceAccountId, "and region ", sourceRegion)
    dbInstanceList = getAllDbInstances(sourceAccountId, sourceRegion)
    if dbInstanceList != []:
        for dbInstance in dbInstanceList:
            resourceArn = dbInstance['DBInstanceArn']
            print("resourceARN: ", resourceArn)
            dbId = dbInstance['DBInstanceIdentifier']
            print("getting all tags for ", dbId)
            tagList = getAllTags(sourceAccountId, sourceRegion, resourceArn)

            if tagList != []:
                print("Checking ClarityId and CostCenter in tags")
                instanceEligible = checkTags(tagList, clarityId, costCenter)

                if instanceEligible:
                    print("Fetching All Snapshots for ", dbId)
                    snapshotList = getSnapshotList(sourceAccountId, sourceRegion, dbId)

                    if snapshotList != []:
                        print("Getting Latest Snapshot")

                        snapshotList = sortSnapshots(snapshotList)
                        snapshotId = snapshotList["SnapshotId"]
                        snapshotArn = snapshotList["DBSnapshotArn"]	
                        print("Arn of snapshot for getting tags", snapshotArn)
                        tags = getTags(sourceAccountId, sourceRegion, snapshotArn)

                        print("Latest Snapshot : ", snapshotId)

                        print("Copying Snapshot ", snapshotId, "in the source region of source account", sourceRegion)
                        id = "copy-of-" + dbId
                        copiedSnapshotId = copySnapshot(sourceAccountId, sourceRegion, sourceRegion, snapshotId,
                                                        stagingKey, id, True)
                        print("Copied Snapshot Id: ", copiedSnapshotId)
                        print("Checking status of copy operation")
                        status = checkStatusOfSnapshot(copiedSnapshotId, sourceRegion, sourceAccountId)

                        if status == 'Completed':
                            print("Sharing the snapshot with target account source region")
                            sharedSnapshotId = shareSnapshot(sourceAccountId, targetAccountId, sourceRegion,
                                                             copiedSnapshotId)
                            print("Checking status of share operation in target account")
                            print("sharedSnapshotId: ", sharedSnapshotId)
                            shareStatus = checkStatusOfSnapshot(sharedSnapshotId, sourceRegion, sourceAccountId)

                            if shareStatus == 'Completed':
                                print("Local copy in target account source region")

                                id = "copy-of-" + dbId + "-" + sourceAccountId
                                snapshotIdSource = copySnapshot(targetAccountId, sourceRegion, sourceRegion,
                                                                sharedSnapshotId, '', id, False)
                                print("Checking status of copy operation in target account source region")
                                print("snapshotIdSource: ", snapshotIdSource)
                                copySourceStatus = checkStatusOfSnapshot(snapshotIdSource, sourceRegion,
                                                                         targetAccountId)

                                if copySourceStatus == 'Completed':
                                    print("Adding tags to the shared snapshot")
                                    #snapshotArn = "arn:aws:rds:eu-west-1:783858292221:snapshot:" + snapshotIdSource
                                    snapshotArn = "arn:aws:rds:"+sourceRegion+":"+targetAccountId+":snapshot:"+snapshotIdSource
                                    addTags(targetAccountId, sourceRegion, tags, snapshotArn)

                                    print("Final Copy Snapshot  in the target  region of target account")
                                    id = "copy-of-" + dbId + "-" + targetAccountId
                                    
                                    snapshotIdTarget = copySnapshot(targetAccountId, sourceRegion, targetRegion,
                                                                    snapshotArn, '', id, True)
                                    print("Copied Snapshot Id intarget account: ", snapshotIdTarget)
                                    print("Checking status of copy operation")
                                    copyTargetStatus = checkStatusOfSnapshot(snapshotIdTarget, targetRegion,
                                                                             targetAccountId)

                                    if copyTargetStatus == 'Completed':
                                        print("Copy Operation Completed Successfully. snapshot ", snapshotIdTarget,
                                              "copied in region: ", targetRegion)
                                    else:
                                        print(copyTargetStatus)
                                else:
                                    print(copySourceStatus)
                            else:
                                print(shareStatus)

                        else:
                            print(status)
                    else:
                        print("No Snapshot available for ", dbId)
                else:
                    print("Clarity Id or Cost Center does not match the input provided for: ", dbId)
            else:
                print("No tags on ", dbId)
    else:
        print("No DB Instance in ", sourceRegion)


##################### Calling main Function ##########################


sourceAccountId = str(sys.argv[1])
targetAccountId = str(sys.argv[2])
sourceRegion = str(sys.argv[3])
targetRegion = str(sys.argv[4])
roleName = str(sys.argv[5])
# targetRoleName = 'replicatesnapshot'
clarityId = str(sys.argv[6])
costCenter = str(sys.argv[7])
key = str(sys.argv[8])
stagingKey = 'alias/' + key

print(" Start DR Operation ")
mainFunc()

print("DR Operation Completed ")


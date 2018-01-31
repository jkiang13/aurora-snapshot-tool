'''
Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

    http://aws.amazon.com/apache2.0/

or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
'''

# copy_automated_snapshots_aurora
# This lambda function copies existing automated Aurora cluster snapshots that may already exist
# in the account using the specified kms encryption key. You can use this if your cluster is encrypted
# with the default account rds encryption key and is already taking automated snapshots.
# take_snapshots_aurora doesn't work with the default encryption key since it can't be shared
# across account whereas this will copy the existing automated snapshots rencrypting using the specified
# KMS_KEY from the environment.s

import boto3
from datetime import datetime
import time
import os
import logging
import re
from snapshots_tool_utils import *

# Initialize everything
LOGLEVEL = os.getenv('LOG_LEVEL').strip()

if os.getenv('REGION_OVERRIDE', 'NO') != 'NO':
    REGION = os.getenv('REGION_OVERRIDE').strip()
else:
    REGION = os.getenv('AWS_DEFAULT_REGION')

KMS_KEY = os.getenv('KMS_KEY').strip()

logger = logging.getLogger()
logger.setLevel(LOGLEVEL.upper())

def lambda_handler(event, context):
    client = boto3.client('rds', region_name=REGION)

    now = datetime.now()
    timestamp_format = now.strftime('%Y-%m-%d-%H-%M')

    snapshots = paginate_api_call(client, 'describe_db_cluster_snapshots', 'DBClusterSnapshots')['DBClusterSnapshots']

    # cluster identifier -> list of snapshots ordered by creation time ascending
    automated_snapshots = {}
    manual_snapshot_identifiers = set()

    for snapshot in snapshots:
        if snapshot['SnapshotType'] == 'automated':
            automated_snapshots.setdefault(snapshot['DBClusterIdentifier'], []).append(snapshot)
        elif snapshot['SnapshotType'] == 'manual':
            manual_snapshot_identifiers.add(snapshot['DBClusterSnapshotIdentifier'])

    for automated_snapshots_by_cluster in automated_snapshots.values():
        automated_snapshots_by_cluster.sort(key=lambda s: s['SnapshotCreateTime'])

        most_recent_automated_snapshot = automated_snapshots_by_cluster[-1]

        # rds snapshots will be prefixed with rds:
        # we want our manual copy to be named without rds: which will work with the rest of this tool
        source_db_cluster_snapshot_identifier = most_recent_automated_snapshot['DBClusterSnapshotIdentifier']
        target_db_cluster_snapshot_identifier = source_db_cluster_snapshot_identifier.split(':')[-1]

        if not target_db_cluster_snapshot_identifier in manual_snapshot_identifiers:
            # only make a copy that does not appear to already exist$
            logging.info("Copying %s to %s", source_db_cluster_snapshot_identifier, target_db_cluster_snapshot_identifier)
            client.copy_db_cluster_snapshot(
                SourceDBClusterSnapshotIdentifier=source_db_cluster_snapshot_identifier,
                TargetDBClusterSnapshotIdentifier=target_db_cluster_snapshot_identifier,
                KmsKeyId=KMS_KEY,
                Tags=[
                    {
                        'Key': 'CreatedBy',
                        'Value': 'Snapshot Tool for Aurora'
                    },
                    {
                        'Key': 'CreatedOn',
                        'Value': timestamp_format
                    },
                    {
                        'Key': 'shareAndCopy',
                        'Value': 'YES'
                    }
                ]
            )

if __name__ == '__main__':
    lambda_handler(None, None)


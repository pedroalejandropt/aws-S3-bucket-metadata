import os 
import gzip
import shutil
from boto3 import client, resource

##ACCESS_KEY
param_1= 'ACCESS_KEY' 
##SECRET_KEY
param_2= 'SECRET_KEY' 
##ENDPOINT_URL
param_3= 'ENDPOINT_URL' 
##BUCKET_NAME
param_4= 'BUCKET_NAME' 

#Create the S3 client
s3_client_resource = client(
    service_name='s3', 
    endpoint_url= param_3,
    aws_access_key_id= param_1,
    aws_secret_access_key= param_2,
    )

s3_resource = resource(
    service_name='s3', 
    endpoint_url= param_3,
    aws_access_key_id= param_1,
    aws_secret_access_key= param_2,
    )

"""
params: 
    file_path

This function unzip the gz files a save it in the temporal directory
"""
def unzip_file(file_path):
    with gzip.open(file_path, 'r') as file_in, open(file_path.strip(".gz"), 'wb') as file_out:
        shutil.copyfileobj(file_in, file_out)

"""
params: 
    bucket_name
    object_name 

This function get the 'ContentType' metadata for a given existing object
"""
def get_content_type(object_name):
    if (object_name.endswith('.mol')):
        return 'chemical/x-mdl-molfile'
    if (object_name.endswith('.png')):
        return 'image/png'
    if (object_name.endswith('.cdx')):
        return 'chemical/x-mdl-molfile'
    return 'application/octet-stream'
    
"""
params: 
    bucket_name
    object_name 

This function build a list of objects per bucket
"""
def build_object_list_per_bucket(variablebucket):
    global list_of_objects_to_be_analyzed
    list_of_objects_to_be_analyzed = []
    start_page = 0
    end_page = 10
    extensions = ['.png','.mol', '.cdx']
    paginator = s3_client_resource.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=variablebucket)

    for num, page in enumerate(pages):
        if (num >= start_page and num < end_page):
            for obj in page['Contents']:
                one_more_object= obj['Key']
                if one_more_object.endswith(tuple(extensions)):
                    list_of_objects_to_be_analyzed.append(one_more_object)
        elif(num == end_page):
            return list_of_objects_to_be_analyzed

"""
params: 
    bucket_name
    object_name 

This function create new metadata for a given existing object
"""
def create_metadata(bucket_name,object_name):
    s3_client_resource.upload_file(object_name, bucket_name, object_name, ExtraArgs={"Metadata": { """ add metadata attributes """ }})

"""
params: 
    bucket_name
    object_name 

This function add new metadata for a given existing object
"""
def add_metadata(bucket_name,object_name):
    object_header = s3_client_resource.head_object(Bucket = bucket_name, Key = object_name)
    metadata = object_header['Metadata']
    metadata['new_metadata'] = 'new_value'
    s3_client_resource.copy_object(Bucket = bucket_name, Key = object_name, CopySource = bucket_name + '/' + object_name, Metadata = metadata, MetadataDirective='REPLACE')

"""
params: 
    bucket_name
    object_name 

This function update a metadata with a new value for a given existing object
"""
def change_metadata(bucket_name, object_name):
    object_header = s3_client_resource.head_object(Bucket = bucket_name, Key = object_name)
    metadata = object_header['Metadata']
    metadata.update({'old_metadata':'new_value'})
    s3_client_resource.copy_object(Bucket = bucket_name, Key = object_name, CopySource = bucket_name + '/' + object_name, Metadata = metadata, MetadataDirective='REPLACE')

"""
params: 
    bucket_name
    object_name 

This function update a system metadata "Content Disposition" with a attachment as value for a given existing object
"""
def change_system_metadata(bucket_name, object_name, old_metadata = None):
    object = s3_resource.Object(bucket_name, object_name)
    object_header = s3_client_resource.head_object(Bucket = bucket_name, Key = object_name)
    metadata = object_header['Metadata'] 
    content_type = get_content_type(object_name)
    actual_metadata = metadata if old_metadata == None else old_metadata
    object.copy_from(CopySource = bucket_name + '/' + object_name,
                     Metadata = actual_metadata,
                     ContentDisposition = 'attachment', 
                     ContentType = content_type, 
                     MetadataDirective='REPLACE')
    
"""
params: 
    bucket_name
    object_name 

This function remove the system metadata "Content Encoding" for a given existing object
"""
def remove_content_encoding(bucket_name, object_name):
    os.makedirs(os.path.dirname('tmp/' + object_name), exist_ok=True)
    ## GET OLD METADATA
    object_header = s3_client_resource.head_object(Bucket = bucket_name, Key = object_name)
    metadata = object_header['Metadata'] 

    s3_client_resource.download_file(bucket_name, object_name, 'tmp/' + object_name + '.gz')
    
    unzip_file('tmp/' + object_name + '.gz')

    s3_client_resource.upload_file('tmp/' + object_name, bucket_name, object_name)
    
    ## ADD OLD METADATA
    change_system_metadata(bucket_name, object_name, metadata)

"""
params: 
    bucket_name
    object_name 

This function print all the metadata by object
"""
def read_metadata (s3_object):
    if 'ContentDisposition' in s3_object:
        s3_object_metadata = s3_object['ContentDisposition']
        print(s3_object_metadata)
    else: 
        print(s3_object)

## program 
print('Start process!')
build_object_list_per_bucket(param_4)
for index, object_name  in enumerate(list_of_objects_to_be_analyzed):
    os.makedirs('tmp', exist_ok=True)
    print(f'{index + 1} - {object_name}')
    s3_object = s3_client_resource.get_object(Bucket=param_4, Key=object_name)
    if 'ContentEncoding' in s3_object:
        remove_content_encoding(param_4, object_name)
    else: 
        change_system_metadata(param_4, object_name)
    shutil.rmtree('tmp')
print('End process!')
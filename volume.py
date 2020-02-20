import boto3
import json
import datetime
from statistics import mean 
import numpy as np
from nested_lookup import nested_lookup
import csv
import pandas as pd
import threading
import os
import argparse



def get_service_client(service,region):
	if region != None:
		try:	
			client = boto3.client(service, region_name=region)
		except ClientError as e:
			print(e)
			sys.exit(1)
		else:
			return client
	else:
		print("Region is required")
		sys.exit(1)


## Function to get list of volumes per region
def get_list_volumes(region):
	client = get_service_client("ec2",region)
	paginator = client.get_paginator('describe_volumes')
	response_iterator = paginator.paginate()
	vol_list= []
	print("Fetching Volume Details from AWS Started")
	for page in response_iterator:
		for i in page['Volumes']:
			if i.get('Tags') != None:
				for tag in i['Tags']:
					if tag['Key'] == "Name":
						name = tag['Value']
						break
					else:
						name = "NaN"
			else:
				name = "NaN"
			vol_list.append({
				'AZ': i['AvailabilityZone'],
				'VolumeID': i['VolumeId'],
				'VolumeName': name,
				'AttachedPath': i['Attachments'][0]['Device'] if i['Attachments'] != [] else None,
				'VolumeType': i['VolumeType'],
				'Size':	i['Size'],
				'IOPS': i['Iops'] if i.get('Iops') != None else None,
				'AttachedInstance': i['Attachments'][0]['InstanceId'] if i['Attachments'] != [] else None,
				'VolumeType': i['VolumeType']
				})				
	with open('vollist-{}.csv'.format(region), 'w') as file:
	    csvwriter = csv.writer(file)
	    csvwriter.writerow(["VolumeID","VolumeName","AZ","VolumeType","Size","IOPS","AttachedPath","InstanceID"])
	    for i in vol_list:
	        csvwriter.writerow([i['VolumeID'],i["VolumeName"],i['AZ'],i['VolumeType'],i['Size'],i['IOPS'],i['AttachedPath'],i['AttachedInstance']]
                       )
	print("Fetching Volume Details from AWS Completed")

## Function to get list of instance IDs from VolumeListCSV
def getinstanceIDs(region):
	volume_data = pd.read_csv("vollist-{}.csv".format(region))
	instancelist = []
	for i in volume_data["InstanceID"]:
		instancelist.append(i)
	return instancelist

## Function to gget instance Name given instanceID
def getinstancename(region,client,instanceid):
	if  str(instanceid) != "nan":
		response = client.describe_tags(
			Filters=[{'Name': "resource-type", 'Values':["instance"]},
			{'Name': "key",'Values': ["Name"]},
			{'Name': "resource-id", 'Values': [instanceid]}]
		)			
		for i in response['Tags']:
			return i['Value']
	return None
## Fucntion to create CSV with instance ID and InstanceName
def instancelistcsv(region):
	print("Fetching Instance Name for EBS Started")
	client = get_service_client("ec2",region)
	instancelist = getinstanceIDs(region)
	with open('instancelist-{}.csv'.format(region), 'w') as file:
	    csvwriter = csv.writer(file)
	    csvwriter.writerow(["InstanceID","InstanceName"])
	    for i in instancelist:
	    	if  str(i) != "nan":
		        csvwriter.writerow([i, getinstancename(region,client,i)])
	print("Fetching Instance Name for EBS Completed")

##Function to Get OPS Usage for different hours and days passed as parameters. This is used for ReadOps and WriteOps
def getops(region,metricName,volumeid,_days,_hours): #hours/days
	timediff=datetime.timedelta(days=_days,hours=_hours)
	end=datetime.datetime.utcnow()
	client = get_service_client('cloudwatch',region)
	if _days > 1:
		period = 3600
	else: 
		period=300
	response = client.get_metric_statistics(
	    Namespace="AWS/EBS",
	    MetricName=metricName,
	    Dimensions=[
	        {
	            'Name': "VolumeId",
	            'Value': volumeid
	        },
	    ],
	    EndTime = end,
	    StartTime = end-timediff,
	    Period = period,
	    Statistics=["Sum"],
	    Unit="Count"
	)
	datapoints=[]
	for i in response['Datapoints']:
		datapoints.append(i['Sum'])
	return datapoints

## Calcuate Max of datapoints given by getops function
def getupperquartile(datapoints):
	if datapoints == []:
		return 0
	return  np.percentile(datapoints,100)

## Calculate IOPS using ReadOps and WriteOps
def iopsused(_read,_write,days,hours):
	if days > 1 and days<=30:
		return (_read + _write)/(3600)
	elif days == 0 and hours >= 1 and hours <=24:
		return (_read + _write)/(300)

## Function to Get io1 & gp2 volumeIDs from VolumeListCSV for IOPS calculation
def volume_io1_gp2_ids(region):
	volume_data = pd.read_csv("vollist-{}.csv".format(region))
	vol = []
	for i in volume_data[["VolumeType","VolumeID"]].get_values():
		if i[0] == "io1" or i[0] == "gp2":
		# if i[0] == "io1":
			vol.append(i[1])
	return vol

## Function to calculate OPS Usage for 24hours
def opsusage24hours(region):
	print("Fetching IOPS Usage list for 24hours Started")
	hours=24
	days=0
	with open('iops24hours-{}.csv'.format(region), 'w') as file:
	    csvwriter = csv.writer(file)
	    csvwriter.writerow(["VolumeID","IOPS-Usage"])
	    for i in volume_io1_gp2_ids(region):
			_read = getupperquartile(getops(region,"VolumeReadOps",i,days,hours))
			_write = getupperquartile(getops(region,"VolumeWriteOps",i,days,hours))
			csvwriter = csv.writer(file)
			csvwriter.writerow([i,iopsused(_read,_write,days,hours)])
	print("Fetching IOPS Usage list for 24hours Completed")

## Function to calculate OPS Usage for 30Days
def opsusage30days(region):
	print("Fetching IOPS Usage list for 30Days Started")
	hours=0
	days=30
	with open('iops30days-{}.csv'.format(region), 'w') as file:
	    csvwriter = csv.writer(file)
	    csvwriter.writerow(["VolumeID","IOPS-Usage"])
	    for i in volume_io1_gp2_ids(region):
			_read = getupperquartile(getops(region,"VolumeReadOps",i,days,hours))
			_write = getupperquartile(getops(region,"VolumeWriteOps",i,days,hours))
			csvwriter = csv.writer(file)
			csvwriter.writerow([i,iopsused(_read,_write,days,hours)])
	print("Fetching IOPS Usage list for 30Days Completed")

##Function to return Region Name as per Pricing
def regionmap(region):
	region_map = {
	'us-east-1': "US East (N. Virginia)",
	'us-west-2': "US West (Oregon)",
	'ap-northeast-1': "Asia Pacific (Tokyo)",
	'ap-southeast-1': "Asia Pacific (Singapore)",
	'eu-central-1': "EU (Frankfurt)",
	'eu-west-1': "EU (Ireland)"
	}
	return region_map[region]

## Function to Get Price of GP2 and ST1
def getpricegp2_st1(region,vtype):
	client = get_service_client('pricing',"us-east-1")
	response = client.get_products(
	    ServiceCode='AmazonEC2',
	    Filters=[
	        {
            'Type': 'TERM_MATCH',
            'Field': 'volumeApiName',
            'Value': vtype
       		 },
	        {
	            'Type': 'TERM_MATCH',
	            'Field': 'location',
	            'Value': regionmap(region)
	        }
	        ]
	        )
	pricelist = json.loads(response['PriceList'][0])
	price = nested_lookup('pricePerUnit',pricelist)
	return price[0]['USD']

## Function to Get Price of IO1
def getpriceio1(region):
	client = get_service_client('pricing',"us-east-1")
	response = client.get_products(
    ServiceCode='AmazonEC2',
    Filters=[
        {
            'Type': 'TERM_MATCH',
            'Field': 'volumeApiName',
            'Value': 'io1'
        },
        {
            'Type': 'TERM_MATCH',
            'Field': 'location',
            'Value': regionmap(region)
        }
        ]  )
	productlist = []
	pricelist = []
	for i in response['PriceList']:
		productlist.append(json.loads(i))
	for i in productlist:
	    if nested_lookup('productFamily',i)[0] == "System Operation":
	        pricelist.append({'IOPS': nested_lookup('pricePerUnit',i)[0]['USD'].encode("utf-8")})
	    elif nested_lookup('productFamily',i)[0] == "Storage":
	        pricelist.append({'Disk': nested_lookup('pricePerUnit',i)[0]['USD'].encode("utf-8")})
	return pricelist

## Calculate Price of GP2 with Price*Size
def totalpricegp2(size,price_gb):
	return size*float(price_gb)

## Calculate Price of IO1, with Price*Size + IOPS-Price*IOPs
def totalpriceio1(size,iops,price_gb_iop):
		# return (float(price_gb_iop[1]['Disk']) * size) + (float(price_gb_iop[0]['IOPS']) * iops)
		return (float(nested_lookup('Disk',price_gb_iop)[0]) * size) + (float(nested_lookup('IOPS',price_gb_iop)[0]) * iops)

## Return Price List of All GP2 and IO1 IDs
def volumepricelist(region):
	print("Calculating Price for Each EBS Volumes Started")
	gp2perGBprice = getpricegp2_st1(region,"gp2")
	st1perGBprice = getpricegp2_st1(region,"st1")
	io1price = getpriceio1(region)
	volume_data = pd.read_csv("vollist-{}.csv".format(region))
	pricelist = []
	for i in volume_data[["VolumeType","Size", "IOPS","VolumeID"]].get_values():
		if i[0] == "gp2":
			pricelist.append([i[3],"gp2", totalpricegp2(i[1],gp2perGBprice)])
		elif i[0] == "io1":
			pricelist.append([i[3],"io1",totalpriceio1(i[1],i[2],io1price)])
		# elif i[0] == "standard":
		# 	pricelist.append([i[3],"st1", totalpricegp2(i[1],st1perGBprice)])

	print("Calculating Price for Each EBS Volumes Completed")
	return pricelist
## Create CSV with Volume Type, VolumeID and Price
def volumepricelistcsv(region):
	pricelist = volumepricelist(region)
	with open('volumepricelist-{}.csv'.format(region), 'w') as file:
	    csvwriter = csv.writer(file)
	    csvwriter.writerow(["VolumeID","VolumeType","Price"])
	    for i in pricelist:
	        csvwriter.writerow(i)
## Merge all intermediate CSV as one. This needs to be done to achieve multi-threading
def mergecsv(region):
	merge = []
	volume_data = pd.read_csv("vollist-{}.csv".format(region)).get_values()
	instance_data = pd.read_csv("instancelist-{}.csv".format(region)).get_values()
	iops_data_24 = pd.read_csv("iops24hours-{}.csv".format(region)).get_values()
	iops_data_30 = pd.read_csv("iops30days-{}.csv".format(region)).get_values()
	price_data = pd.read_csv("volumepricelist-{}.csv".format(region)).get_values()
	for i in volume_data:
		l1 = list(i)
		for j in instance_data:
			if i[7] == j[0]:
				l1.append(j[1])
				break
		else:
			l1.append("NaN")
	    
		for k in iops_data_24:
			if i[0] == k[0]:
				l1.append(k[1])
				break
		else:
			l1.append("NaN")

		for x in iops_data_30:
			if i[0] == x[0]:
				l1.append(x[1])
				break
		else:
			l1.append("NaN")

		for y in price_data:
			if i[0] == y[0]:
				l1.append(y[2])
				break
		else:
			l1.append("NaN")
		merge.append(l1)
    
	with open('vollistcomplete-{}.csv'.format(region), 'w') as file:
		csvwriter = csv.writer(file)
		csvwriter.writerow(['VolumeID','VolumeName',
	  'AZ',
	  'VolumeType',
	  'Size(GB)',
	  'IOPS',
	  'AttachedPath',
	  'InstanceID','InstanceName','IOPSUsage-24hours','IOPSUsage-30days','Price$'])
		for i in merge:
			csvwriter.writerow([row for row in i])
	os.remove("vollist-{}.csv".format(region))
	os.remove("instancelist-{}.csv".format(region))
	os.remove("iops24hours-{}.csv".format(region))
	os.remove("iops30days-{}.csv".format(region))
	os.remove("volumepricelist-{}.csv".format(region))



if __name__ == "__main__":
	parser= argparse.ArgumentParser()
	parser.add_argument('-r','--region', help="Name of Region", required=True, 
		choices=['us-east-1', 'us-west-2', 'eu-west-1', 'eu-central-1', 'ap-southeast-1', 'ap-northeast-1'])
	args=parser.parse_args()
	region = args.region
	t1 = threading.Thread(target=get_list_volumes, args=(region,)) 
	t2 = threading.Thread(target=instancelistcsv, args=(region,))
	t3 = threading.Thread(target=opsusage24hours, args=(region,))
	t4 = threading.Thread(target=opsusage30days, args=(region,))
	t5 = threading.Thread(target=volumepricelistcsv, args=(region,))

## Start VolumeList thread which will be used by all next threads.
	t1.start()
	t1.join() ## wait for thread to finish

## Starts all simultaneous threads
	t2.start()
	t3.start()
	t4.start()
	t5.start()

## Waiting for all threads to finish

	t2.join()
	t3.join()
	t4.join()
	t5.join()

## Merge CSV when all threads are completed
	mergecsv(region)





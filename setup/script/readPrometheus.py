import datetime
import time
import requests
import subprocess
import shlex

PROMETHEUS = "http://localhost:9090/"
QUERIES1 = ['flink_taskmanager_job_task_operator_output_query_1_dayAvgThroughput',
			'flink_taskmanager_job_task_operator_output_query_1_weekAvgThroughput',
			'flink_taskmanager_job_task_operator_output_query_1_monthAvgThroughput',
			'flink_taskmanager_job_task_operator_KafkaProducer_request_latency_avg{job_name="Query_1", operator_name=~"(Sink:_output_query_1_dayorg_apache_flink_streaming_api_windowing_time).*"}',
			'flink_taskmanager_job_task_operator_KafkaProducer_request_latency_avg{job_name="Query_1", operator_name=~"(Sink:_output_query_1_weekorg_apache_flink_streaming_api_windowing_time).*"}',
			'flink_taskmanager_job_task_operator_KafkaProducer_request_latency_avg{job_name="Query_1", operator_name=~"(Sink:_output_query_1_monthorg_apache_flink_streaming_api_windowing_time).*"}']
QUERIES2 = ['flink_taskmanager_job_task_operator_output_query_2_dayAvgThroughput',
			'flink_taskmanager_job_task_operator_output_query_2_weekAvgThroughput',
			'flink_taskmanager_job_task_operator_KafkaProducer_request_latency_avg{job_name="Query_2", operator_name=~"(Sink:_output_query_2_dayorg_apache_flink_streaming_api_windowing_time).*"}',
			'flink_taskmanager_job_task_operator_KafkaProducer_request_latency_avg{job_name="Query_2", operator_name=~"(Sink:_output_query_2_weekorg_apache_flink_streaming_api_windowing_time).*"}']
EPSILON = 0.05

QUERY1_THR_24H = []
QUERY1_THR_7D = []
QUERY1_THR_30D = []
def prom():
	

	# end_of_month = datetime.datetime.today().replace(day=1).date()

	# last_day = end_of_month - datetime.timedelta(days=1)
	# duration = "[" + str(last_day.day) + "d]"

	# try:
	# response = requests.get(PROMETHEUS + "/api/v1/query", params={"query": "flink_taskmanager_job_task_operator_output_query_1_monthAvgThroughput"}) 
	response = requests.get(PROMETHEUS + "/api/v1/query", params={"query": 'flink_taskmanager_job_task_operator_KafkaProducer_request_latency_avg{job_name="Query_1", operator_name=~"(Sink:_output_query_1_dayorg_apache_flink_streaming_api_windowing_time).*"}'}) 
	# "flink_taskmanager_job_task_operator_KafkaProducer_request_latency_avg{job_name=\"Query_1\", operator_name=~\"(Sink:_output_query_1_dayorg_apache_flink_streaming_api_windowing_time).*\"}"
	# # response = requests.get(PROMETHEUS + '/metrics',
	# #   params={
	# #     'query': 'sum by (job)(increase(process_cpu_seconds_total' + duration + '))',
	# #     'time': time.mktime(end_of_month.timetuple())})
	print(response.json())
	results = response.json()['data']['result']
	if len(results) == 0:
		print("not yet")

	# except:


	# print(response.json())
	# print("{:%B %Y}:".format(last_day))
	# for result in results:
	result = results[-1]
	print(" {metric}: {value[1]}".format(**result) + '\n')
	# print(result["{metric}".format(**result)])
	for key, value in result.items():
		print("key: " + str(key) + ", value: " + str(value))
		print()
	print(result['value'][1])

def makeQuery(query):
	response = requests.get(PROMETHEUS + "/api/v1/query", params={"query": query}) 
	
	results = response.json()['data']['result']
	if len(results) == 0:
		return None
	result = results[-1]
	return result['value'][1]

def checkStop(condition):
	res = False
	for e in condition:
		if e == True:
			res = True
	return res

def askProm(queries):
	output = []
	notEnough = []
	for i in range(len(queries)):
		output.append([])
		notEnough.append(True)
	# notEnough = True
	while notEnough:
		# wait 
		time.sleep(0.5)
		# query value
		for i in range(len(queries)):
			if notEnough[i] == True:
				res = makeQuery(queries[i])
				# save value
				if res != None and res !='NaN':
					output[i].append(res)
					# need to stop?
					if (len(output[i])>2):
						if (float(output[i][-1]) - float(output[i][-2]) < EPSILON):
							notEnough[i] = False
	return output

def singleIteration():
	# create containers, wait to finish
	process = subprocess.run(shlex.split("docker-compose up -d"), 
                         stdout=subprocess.PIPE, 
                         universal_newlines=True)
	# exec query 1, run in background
	process = subprocess.Popen(shlex.split("docker-compose exec flink_client /bin/bash /script/runQ1.sh"), 
                         stdout=subprocess.PIPE, 
                         universal_newlines=True)
	outQ1 = askProm(QUERIES1)
	print(outQ1[-1][-1])

	# exec query 2
	# process = subprocess.run(shlex.split("docker-compose exec flink_client /bin/bash /script/runQ2.sh"), 
 #                         stdout=subprocess.PIPE, 
 #                         universal_newlines=True)
	# askProm(QUERIES2)

	# kill containers
	process = subprocess.run(shlex.split("docker-compose down -v"), 
                         stdout=subprocess.PIPE, 
                         universal_newlines=True)

def main():
	process = subprocess.run(['echo', 'Even more output'], 
                         stdout=subprocess.PIPE, 
                         universal_newlines=True)
	print(process.stdout)
	process = subprocess.Popen(['echo', 'More output'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	stdout, stderr = process.communicate()
	print(stdout)

# https://janakiev.com/blog/python-shell-commands/
# process = subprocess.Popen(['ping', '-c 4', 'python.org'], 
#                            stdout=subprocess.PIPE,
#                            universal_newlines=True)
# When you run .communicate(), it will wait until the process is complete. However if you have a long program that you want to run and you want to continuously check the status in realtime while doing something else, you can do this like here:
# while True:
#     output = process.stdout.readline()
#     print(output.strip())
#     # Do something else
#     return_code = process.poll()
#     if return_code is not None:
#         print('RETURN CODE', return_code)
#         # Process has finished, read rest of the output 
#         for output in process.stdout.readlines():
#             print(output.strip())
#         break


if __name__ == "__main__":
	singleIteration()
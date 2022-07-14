from datetime import datetime, timedelta
from fileinput import filename
from os import listdir
import re
import time
from numpy import array_equal, true_divide
import mysql.connector

# where the report (account) files are at
report_dir = 'accounting'
# put those report fiels already processed here
processed_report_files = []
# a list to keep track of all added job objects
jobs_list = []

# jobs class
class Job:

    def __init__(self, ):
        #self.user_id = 0                   # not known
        self.requestor = ''                 # it's always maui@hpc.ce.sharif.edu when requestor. Otherwise, user
        self.job_id = 0                     # job ID
        self.job_name = ''                  # job name
        self.queue_name = ''                # job queue
        #self.output = ''                   # not known
        #self.error = ''                    # not known
        self.gpu_requested = 0              # Resource_List.neednodes=X:ppn=Y:gpus=Z
        self.gpu_used = 0                   # Resource_List.nodes=X:ppn=Y:gpus=Z
        self.mem_requested = 0              # Resource_List.mem
        self.mem_usage = ''                 # resources_used.mem
        self.cpu_requested = 0              # Resource_List.neednodes=X:ppn=Y
        self.cpu_usage = ''                 # resources_used.cput
        self.vmem_usage = ''                # resources_used.vmem
        self.exec_host = ''                 # exec_host
        self.need_nodes = ''                # Resource_List.neednodes
        self.resource_nodes = ''            # Resource_List.nodes
        self.session = 0                    # session
        self.status = ''                    # job status
        self.submitted = ''                 # the one with 'Q' status in log file (e.g., 00:00:03;Q;495164)
        self.created = ''                   # ctime
        self.started = ''                   # start
        self.finished = ''                  # end
        #self.last_modified = ''            # not known
        #self.processed = ''
        #self.archive

    def __str__(self):
        return f'Job ID: {self.job_id},\n\
                User: {self.requestor},\n\
                Job Name: {self.job_name}\n\
                Queue: {self.queue_name}\n\
                GPU_Req: {self.gpu_requested}\n\
                GPU_Use: {self.gpu_used}\n\
                Mem_Req: {self.mem_requested}\n\
                Mem_Use: {self.mem_usage}\n\
                CPU_Req: {self.cpu_requested}\n\
                CPU_Use: {self.cpu_usage}\n\
                Vmem_Use: {self.vmem_usage}\n\
                Exec_Host: {self.exec_host}\n\
                Need_Nodes: {self.need_nodes}\n\
                Nodes: {self.resource_nodes}\n\
                Session: {self.session}\n\
                Status: {self.status}\n\
                Submitted: {self.submitted}\n\
                Created: {self.created}\n\
                Started: {self.started}\n\
                Finished: {self.finished}'


def update_database(job):
    if job in jobs_list:
        sql = "UPDATE jobs SET requestor=%s, job_name=%s, queue_name=%s, gpu_requested=%s, gpu_used=%s, \
                mem_requested=%s, mem_usage=%s, cpu_requested=%s, cpu_usage=%s, vmem_usage=%s, exec_host=%s, need_nodes=%s, \
                resource_nodes=%s, session=%s, status=%s, submitted=%s, created=%s, started=%s, finished=%s \
                WHERE job_id=%s;"
        val = (job.requestor, job.job_name, job.queue_name, job.gpu_requested, job.gpu_used, job.mem_requested, \
                job.mem_usage, job.cpu_requested, job.cpu_usage, job.vmem_usage, job.exec_host, job.need_nodes, job.resource_nodes, \
                    job.session, job.status, job.submitted, job.created, job.started, job.finished, job.job_id)
        mycursor.execute(sql, val)
        mydb.commit()
    # new job
    else:
        sql = "INSERT INTO jobs (requestor, job_id, job_name, queue_name, \
                gpu_requested, gpu_used, mem_requested, mem_usage, cpu_requested, \
                cpu_usage, vmem_usage, exec_host, need_nodes, resource_nodes, session, status, \
                submitted, created, started, finished) \
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, \
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
        val = (job.requestor, job.job_id, job.job_name, job.queue_name, job.gpu_requested, job.gpu_used, job.mem_requested, \
                job.mem_usage, job.cpu_requested, job.cpu_usage, job.vmem_usage, job.exec_host, job.need_nodes, job.resource_nodes, \
                    job.session, job.status, job.submitted, job.created, job.started, job.finished)
        mycursor.execute(sql, val)
        mydb.commit()

def process_file(file_name):
    file = open(report_dir + '/' + file_name, 'r')
    # extract file date
    file_date = datetime.strptime(file_name, "%Y%m%d")
    print('Processing log file from: {0}'.format(file_date.date()))

    line = file.readline()
    while line:
        # strip newline chracters off of each line
        line = line.strip('\n')
        # split each line by space character (the first element is date and not needed)
        array = line.split(' ')[1:]
        # extract the log status from log file (Q, S, D, E)
        log_status = re.findall(";.;", array[0])[0].strip(';')

        # we have nothing to do with D status as it bears no additional useful info
        if log_status == 'D':
            line = file.readline()
            continue

        # create an instance of the Job class
        job = Job()

        try:
            # set job id of this object
            job.job_id = re.findall(";[0-9]+", array[0])[0].strip(';')
        except:
            line = file.readline()
            continue

        # items to be extracted: submit time, job_id, queue
        if log_status == 'Q':
            job.status = 'Q'
            q_items = array[0].split(';')
            # set submission time for this job (date + time)
            job.submitted = str(file_date.date()) + '\n' + q_items[0]
            # extract queue name
            job.queue_name = q_items[3][q_items[3].find('=')+1:]
            # add job to the list of visited objects (job ID's)
            update_database(job)
            jobs_list.append(job)

        # items to be extracted: requestor (user), job_name, created, start datetime, exec_host, neednodes,\
        # gpu_requested, gpu, mem_requested, mem_usage nodes
        elif log_status == 'S':
            if len(array) == 14 or len(array) == 15:
                # this is for when we have a job ID that does not start with Q status
                new_job_flag = False
                # if memory has been specified
                has_mem = False
                if not search_job_id(job.job_id):
                    job.queue_name = array[3][array[3].find('=')+1:]
                    new_job_flag = True
            
                job.status = 'S'
                job.requestor = array[0][array[0].find('=')+1:]
                job.job_name = array[2][array[2].find('=')+1:]

                # extract the creat timetamp
                c_timestamp = array[4][array[4].find('=')+1:]
                c_datetime = ts2dt(c_timestamp)
                job.created = str(c_datetime.date()) + '\n' + str(c_datetime.time())

                # extract start timestamp
                s_timestamp = array[7][array[7].find('=')+1:]
                s_datetime = ts2dt(s_timestamp)
                job.started = str(s_datetime.date()) + '\n' + str(s_datetime.time())
                
                # extract exec_host
                job.exec_host = (array[9][:array[9].find('/')])[array[9].find('=')+1:]

                # some log files have a length of 14 and some 15. They should be separated
                if 'neednodes' in array[10]:
                    # extract number of nodes needed
                    job.need_nodes = array[10][array[10].find('=')+1:array[10].find(':')]
                    if 'gpus' in array[10]:
                        # extract number of gpus requested
                        job.gpu_requested = array[10][array[10].find('gpus')+5:]
                        try:
                            job.gpu_used = array[12][array[12].find('gpus')+5:]
                        except:
                            pass
                    try:
                        job.resource_nodes = array[12][array[12].find('=')+1:array[12].find(':')]
                    except:
                        # in case no assigned node was found
                        pass
            
                if 'mem' in array[10]:
                    has_mem = True
                    # extract theh amount of memory requestd (string now, could be improved)
                    job.mem_requested = array[10][array[10].find('=')+1:]
                    # extract number of nodes needed
                    job.need_nodes = array[11][array[11].find('=')+1:array[11].find(':')]
                    if 'gpus' in array[11]:
                        # extract number of gpus requested
                        job.gpu_requested = array[11][array[11].find('gpus')+5:]
                        try:
                            job.gpu_used = array[13][array[13].find('gpus')+5:]
                        except:
                            pass
                    try:
                        job.resource_nodes = array[13][array[13].find('=')+1:array[13].find(':')]
                    except:
                        # in case no assigned node was found
                        pass
                

                if new_job_flag:
                    # add to list as a new job
                    update_database(job)
                    jobs_list.append(job)
                    new_job_flag = False
                else:
                    # update the previous job (with Q status)
                    previous_job = search_job(job.job_id)
                    previous_job.requestor = job.requestor
                    previous_job.job_name = job.job_name
                    previous_job.created = job.created
                    previous_job.started = job.started
                    previous_job.exec_host = job.exec_host
                    previous_job.need_nodes = job.need_nodes
                    previous_job.resource_nodes = job.resource_nodes
                    previous_job.gpu_requested = job.gpu_requested
                    previous_job.gpu_used = job.gpu_used
                    previous_job.gpu_used = job.gpu_used
                    previous_job.status = job.status
                    if has_mem:
                        previous_job.mem_requested = job.mem_requested
                        has_mem = False
                    update_database(previous_job)

        # items to be extracted: session, end (finished) time, cpu time (cpu_usage), used mem, used vmem   
        elif log_status == 'E':
            if len(array) == 20 or len(array) == 21:
                # this is for when we have a job ID that does not start with Q status
                new_job_flag = False
                # if memory has been specified
                has_mem = False
                if not search_job_id(job.job_id):
                    job.requestor = array[0][array[0].find('=')+1:]
                    job.queue_name = array[3][array[3].find('=')+1:]
                    new_job_flag = True

                job.status = 'E'

                try:
                    if 'session' in array[13]:
                        job.session = array[13][array[13].find('=')+1:]
                        #extract end timestamp
                        e_timestamp = array[14][array[14].find('=')+1:]
                        e_datetime = ts2dt(e_timestamp)
                        job.finished = str(e_datetime.date()) + '\n' + str(e_datetime.time())
                        job.cpu_usage = array[16][array[16].find('=')+1:]
                        job.mem_usage = array[17][array[17].find('=')+1:]
                        job.vmem_usage = array[18][array[18].find('=')+1:]
                    elif 'session' in array[14]:
                        job.session = array[14][array[14].find('=')+1:]
                        #extract end timestamp
                        e_timestamp = array[15][array[15].find('=')+1:]
                        e_datetime = ts2dt(e_timestamp)
                        job.finished = str(e_datetime.date()) + '\n' + str(e_datetime.time())
                        job.cpu_usage = array[17][array[17].find('=')+1:]
                        job.mem_usage = array[18][array[18].find('=')+1:]
                        job.vmem_usage = array[19][array[19].find('=')+1:]
                except:
                    print(job.job_id)
                
                if 'mem' in array[10]:
                    has_mem = True
                    # extract theh amount of memory requestd (string now, could be improved)
                    job.mem_requested = array[10][array[10].find('=')+1:]
                    # extract number of nodes needed
                    job.need_nodes = array[11][array[11].find('=')+1:array[11].find(':')]
                    if 'gpus' in array[11]:
                        # extract number of gpus requested
                        job.gpu_requested = array[11][array[11].find('gpus')+5:]
                        try:
                            job.gpu_used = array[13][array[13].find('gpus')+5:]
                        except:
                            pass
                    try:
                        job.resource_nodes = array[13][array[13].find('=')+1:array[13].find(':')]
                    except:
                        # in case no assigned node was found
                        pass
                
                if new_job_flag:
                    # add to list as a new job
                    update_database(job)
                    jobs_list.append(job)
                    new_job_flag = False
                else:
                    previous_job = search_job(job.job_id)
                    previous_job.session = job.session
                    previous_job.finished = job.finished
                    previous_job.cpu_usage = job.cpu_usage
                    previous_job.mem_usage = job.mem_usage
                    previous_job.vmem_usage = job.vmem_usage
                    previous_job.vmem_usage = job.vmem_usage
                    previous_job.status = job.status
                    if has_mem:
                        previous_job.mem_requested = job.mem_requested
                        has_mem = False
                    update_database(previous_job)
                
        line = file.readline()

def process_most_recent_file(file_name): 
    file = open(report_dir + '/' + file_name, 'r')
    # extract file date
    file_date = datetime.strptime(file_name, "%Y%m%d")
    print('Processing log file from: {0}'.format(file_date.date()))

    while True:
        line = file.readline()
        if not line:
            new_reporting_files = [f for f in listdir(report_dir)]
            new_reporting_files.sort()
            if new_reporting_files[-1] != file_name:
                return new_reporting_files[-1]
            time.sleep(10)
            continue
        else:
            # strip newline chracters off of each line
            line = line.strip('\n')
            # split each line by space character (the first element is date and not needed)
            array = line.split(' ')[1:]
            # extract the log status from log file (Q, S, D, E)
            log_status = re.findall(";.;", array[0])[0].strip(';')

            # we have nothing to do with D status as it bears no additional useful info
            if log_status == 'D':
                continue

            # create an instance of the Job class
            job = Job()

            try:
                # set job id of this object
                job.job_id = re.findall(";[0-9]+", array[0])[0].strip(';')
            except:
                line = file.readline()
                #print('Job ID Prolem: {0}'.format(array))
                continue

            # items to be extracted: submit time, job_id, queue
            if log_status == 'Q':
                job.status = 'Q'
                q_items = array[0].split(';')
                # set submission time for this job (date + time)
                job.submitted = str(file_date.date()) + '\n' + q_items[0]
                # extract queue name
                job.queue_name = q_items[3][q_items[3].find('=')+1:]
                # add job to the list of visited objects (job ID's)
                update_database(job)
                jobs_list.append(job)

            # items to be extracted: requestor (user), job_name, created, start datetime, exec_host, neednodes,\
            # gpu_requested, gpu, mem_requested, mem_usage nodes
            elif log_status == 'S':
                if len(array) == 14 or len(array) == 15:
                    # this is for when we have a job ID that does not start with Q status
                    new_job_flag = False
                    # if memory has been specified
                    has_mem = False
                    if not search_job_id(job.job_id):
                        job.queue_name = array[3][array[3].find('=')+1:]
                        new_job_flag = True
                
                    job.status = 'S'
                    job.requestor = array[0][array[0].find('=')+1:]
                    job.job_name = array[2][array[2].find('=')+1:]

                    # extract the creat timetamp
                    c_timestamp = array[4][array[4].find('=')+1:]
                    c_datetime = ts2dt(c_timestamp)
                    job.created = str(c_datetime.date()) + '\n' + str(c_datetime.time())

                    # extract start timestamp
                    s_timestamp = array[7][array[7].find('=')+1:]
                    s_datetime = ts2dt(s_timestamp)
                    job.started = str(s_datetime.date()) + '\n' + str(s_datetime.time())
                    
                    # extract exec_host
                    job.exec_host = (array[9][:array[9].find('/')])[array[9].find('=')+1:]

                    # some log files have a length of 14 and some 15. They should be separated
                    if 'neednodes' in array[10]:
                        # extract number of nodes needed
                        job.need_nodes = array[10][array[10].find('=')+1:array[10].find(':')]
                        if 'gpus' in array[10]:
                            # extract number of gpus requested
                            job.gpu_requested = array[10][array[10].find('gpus')+5:]
                            try:
                                job.gpu_used = array[12][array[12].find('gpus')+5:]
                            except:
                                pass
                        try:
                            job.resource_nodes = array[12][array[12].find('=')+1:array[12].find(':')]
                        except:
                            # in case no assigned node was found
                            pass
                
                    if 'mem' in array[10]:
                        has_mem = True
                        # extract theh amount of memory requestd (string now, could be improved)
                        job.mem_requested = array[10][array[10].find('=')+1:]
                        # extract number of nodes needed
                        job.need_nodes = array[11][array[11].find('=')+1:array[11].find(':')]
                        if 'gpus' in array[11]:
                            # extract number of gpus requested
                            job.gpu_requested = array[11][array[11].find('gpus')+5:]
                            try:
                                job.gpu_used = array[13][array[13].find('gpus')+5:]
                            except:
                                pass
                        try:
                            job.resource_nodes = array[13][array[13].find('=')+1:array[13].find(':')]
                        except:
                            # in case no assigned node was found
                            pass
                    

                    if new_job_flag:
                        # add to list as a new job
                        update_database(job)
                        jobs_list.append(job)
                        new_job_flag = False
                    else:
                        # update the previous job (with Q status)
                        previous_job = search_job(job.job_id)
                        previous_job.requestor = job.requestor
                        previous_job.job_name = job.job_name
                        previous_job.created = job.created
                        previous_job.started = job.started
                        previous_job.exec_host = job.exec_host
                        previous_job.need_nodes = job.need_nodes
                        previous_job.resource_nodes = job.resource_nodes
                        previous_job.gpu_requested = job.gpu_requested
                        previous_job.gpu_used = job.gpu_used
                        previous_job.gpu_used = job.gpu_used
                        previous_job.status = job.status
                        if has_mem:
                            previous_job.mem_requested = job.mem_requested
                            has_mem = False
                        update_database(previous_job)

            # items to be extracted: session, end (finished) time, cpu time (cpu_usage), used mem, used vmem   
            elif log_status == 'E':
                if len(array) == 20 or len(array) == 21:
                    # this is for when we have a job ID that does not start with Q status
                    new_job_flag = False
                    # if memory has been specified
                    has_mem = False
                    if not search_job_id(job.job_id):
                        job.requestor = array[0][array[0].find('=')+1:]
                        job.queue_name = array[3][array[3].find('=')+1:]
                        new_job_flag = True

                    job.status = 'E'

                    try:
                        if 'session' in array[13]:
                            job.session = array[13][array[13].find('=')+1:]
                            #extract end timestamp
                            e_timestamp = array[14][array[14].find('=')+1:]
                            e_datetime = ts2dt(e_timestamp)
                            job.finished = str(e_datetime.date()) + '\n' + str(e_datetime.time())
                            job.cpu_usage = array[16][array[16].find('=')+1:]
                            job.mem_usage = array[17][array[17].find('=')+1:]
                            job.vmem_usage = array[18][array[18].find('=')+1:]
                        elif 'session' in array[14]:
                            job.session = array[14][array[14].find('=')+1:]
                            #extract end timestamp
                            e_timestamp = array[15][array[15].find('=')+1:]
                            e_datetime = ts2dt(e_timestamp)
                            job.finished = str(e_datetime.date()) + '\n' + str(e_datetime.time())
                            job.cpu_usage = array[17][array[17].find('=')+1:]
                            job.mem_usage = array[18][array[18].find('=')+1:]
                            job.vmem_usage = array[19][array[19].find('=')+1:]
                    except:
                        continue

                    if 'mem' in array[10]:
                        has_mem = True
                        # extract theh amount of memory requestd (string now, could be improved)
                        job.mem_requested = array[10][array[10].find('=')+1:]
                        # extract number of nodes needed
                        job.need_nodes = array[11][array[11].find('=')+1:array[11].find(':')]
                        if 'gpus' in array[11]:
                            # extract number of gpus requested
                            job.gpu_requested = array[11][array[11].find('gpus')+5:]
                            try:
                                job.gpu_used = array[13][array[13].find('gpus')+5:]
                            except:
                                pass
                        try:
                            job.resource_nodes = array[13][array[13].find('=')+1:array[13].find(':')]
                        except:
                            # in case no assigned node was found
                            pass
                    
                    if new_job_flag:
                        # add to list as a new job
                        update_database(job)
                        jobs_list.append(job)
                        new_job_flag = False
                    else:
                        previous_job = search_job(job.job_id)
                        previous_job.session = job.session
                        previous_job.finished = job.finished
                        previous_job.cpu_usage = job.cpu_usage
                        previous_job.mem_usage = job.mem_usage
                        previous_job.vmem_usage = job.vmem_usage
                        previous_job.vmem_usage = job.vmem_usage
                        previous_job.status = job.status
                        if has_mem:
                            previous_job.mem_requested = job.mem_requested
                            has_mem = False
                        update_database(previous_job)
        

def search_job_id(id):
    for job in jobs_list:
        if id == job.job_id:
            return True
    return False

def search_job(id):
    for job in jobs_list:
        if id == job.job_id:
            return job

# converts unix timestamp to datetime format
def ts2dt(ts):
    try:
        ts = int(ts)
    except (ValueError, TypeError):
        return None
    return datetime.fromtimestamp(ts)

def get_cpu_seconds(st):
    times = st.split(':')
    times = [int(t) for t in times]
    return 3600 * times[0] + 60 * times[1] + times[2]


def run():
    # read files inside the accounting directory
    reporting_files = [f for f in listdir(report_dir)]
    reporting_files.sort()
    # the file probably not completetd yet
    last_file = reporting_files.pop()
    
    # all reporting files except for the last one which is not complete yet
    for file in reporting_files:
        if len(reporting_files) > 0:
            if file not in processed_report_files:
                processed_report_files.append(file)
                process_file(file)
                # check to see if a new log file has been added
                time.sleep(3)
                if (len([f for f in listdir(report_dir)])-1) > len(reporting_files):
                    new_reporting_files = [f for f in listdir(report_dir)]
                    new_reporting_files.sort()
                    reporting_files.append(last_file)
                    last_file = new_reporting_files.pop()
    processed_report_files.append(last_file)
    new_last_file = process_most_recent_file(last_file)
    while True:
        new_last_file = process_most_recent_file(new_last_file)
            
    

if __name__ == '__main__':
    
    # connect to test database
    mydb = mysql.connector.connect(
        host="213.233.184.30",
        user="jobs",
        database="test",
        password="sina1234qwer",
        port=3306
    )

    mycursor = mydb.cursor(buffered=True)
    
    
    run()
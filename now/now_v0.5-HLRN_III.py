#! /usr/bin/env python

import os, sys, argparse, socket, getpass
from datetime import datetime,timedelta,date,time

#General variables
version="0.5-HLRN III"
me=getpass.getuser()
host=socket.gethostname()

timestamp=datetime.today()
timestamp_f=str(timestamp).split(".")[0]
 
parser = argparse.ArgumentParser(description=""" now: easy job monitoring. \n """, epilog='\n Usage: now -h\
n')
parser.add_argument('-d','--days', help="Number of days to display for the history.", required=False)
parser.add_argument('-mm','--minimal_out', help="Minimal output (good for parsing values with scripts).", re
quired=False, action='store_true')
parser.add_argument('-lr','--long_rows', help="Print full length rows (paths) instead of shrinked to termina
l width.", required=False, action='store_true')
parser.add_argument('-io','--io_check', help="Check last output access to working directory (for finding zom
bie jobs).", required=False, action='store_true')
args = vars(parser.parse_args())
#inpfile= args['file_name']

if args['days']:
 d=args['days']
else:
 d=1

if not args['minimal_out']:
 print ' \033[05;31mnow v.' + version + ':\033[00m Jobs of user \033[02;36m' + me + ' \033[00m running on \0
33[02;36m' + host  + '\033[00m  ' + timestamp_f

 print " "

# Check if ~/.job_history_* files exists (_r running, _q queued, _d done)
oldjobs_r,oldjobs_q,oldjobs_d,newjobs_r,newjobs_q=[],[],[],[],[]
try:
   open(os.path.expanduser("~"+me) + '/.job_history_p')  #previous jobs
   for job in open(os.path.expanduser("~"+me+'/.job_history_p'),"r"):
    if job.split()[1] == "Running":
     #print "print", job.split("\n")
     oldjobs_r.append(job.split("\n")[0].split())
    elif job.split()[1] == "Idle" or job.split()[1] == "qw":
     oldjobs_q.append(job.split("\n")[0].split())
   open(os.path.expanduser("~"+me) + '/.job_history_d')
   for job in open(os.path.expanduser("~"+me+'/.job_history_d'),"r"):
    oldjobs_d.append(job.split("\n")[0].split())
except:
   print 'No job history file in home directory: '+ os.path.expanduser("~"+me)

#print "oldjobs_r", oldjobs_r

def colorst(x):
 if x == "Running" or x == "t":
  return '\033[42;34m R \033[0m '
 if x == "t":
  return '\033[42;33m R \033[0m '
 if x == "Idle":
  return '\033[47;33m Q \033[0m '
 if x == "Canceling":
  return '\033[46;41m C \033[0m '
 if x == "hqw":
  return '\033[47;35m H \033[0m '
 if x == "Eqw":
  return '\033[40;31mEqw \033[0m '
 if x == "dr" or x == "dhr":
  return '\033[40;32m C \033[0m '

if not args['long_rows']:
 cols=int(os.popen('tput cols').read())-3 #no. of columns in current window
else:
 cols=200

#My running jobs
if not args['minimal_out']:
  print "%10s   %6s  %6s   %3s %20s " %("Job ID (Moab)", "Status", "#Nodes","Wait time","Path to output")

#Convert between ids: checkjob -v -v hannover.29063 | grep ^job

jobids=os.popen('showq -u $USER |grep $USER').readlines()

if len(jobids) > 0:
 for i in jobids:
  np="--"
  jid=i.split()[0]
  #name=i.split()[3]
  status=i.split()[2]
  #if status=="R" or status=="Q":
  np=i.split()[3]
  if status == "Running":
   wait="\033[0;32m -"+i.split()[4]+' \033[0m '
  elif status == "Idle":
   showstart=os.popen('showstart '+ jid + ' |grep "Estimated Rsv based start in"').read().split()[5]
   #print showstart
   wait=' \033[0;31m '+showstart+' \033[0m '
  else:
   wait='            '
 
  try:
   path=os.popen('checkjob -v -v ' + jid + ' |grep SubmitDir:').read().split()[1]
   #path=os.popen('qstat -j ' + jid + ' |grep cwd').read().split()[1]
  except:
    try:
      path=os.popen('checkjob -v -v ' + jid + ' |grep IWD:').read().split()[1]
    except:
      path=""
  longpath=path
  if len(path) > cols-30:
   path=".../"+"/".join(path.split('/')[5:])
   
  print "%s    %12s   %3s   %s  %s" %(jid, colorst(status),np,wait,path)
  #print "%7s  %3s  %10s  %s %s " %(jid, np, name,colorst(status),path)
  if status == "Running" or status == "t":
   newjobs_r.append([jid,status,longpath,timestamp])
  elif status == "Idle" or status == "hqw":
   newjobs_q.append([jid,status,longpath,timestamp])

else:
 print "  ***  NO JOBS RUNNING  *** "
 

#if mm == False:
# print "-"*cols
# print ""

jidsold,jidsnew=[],[]
newjobs=newjobs_r+newjobs_q  #Jobs running currently
if len(newjobs) > 0:
 for i in zip(*newjobs)[0]:
  jidsnew.append(i)
else:
 jidsnew=[]

oldjobs=oldjobs_r+oldjobs_q  # jobs in .job_history_p (previous, not completed)
if len(oldjobs) > 0:
 for i in zip(*oldjobs)[0]:
  jidsold.append(i)
else:
  jidsold=[]

if not args['minimal_out']:
 if len(set(jidsold) - set(jidsnew)) > 0:
  print ""
  print "-"*cols
  print " "
  print " \033[01;31m ### Just finished job(s) !!! ###  \033[00m "
  for job in set(jidsold) - set(jidsnew): # Just finished jobs
   curjobid=jidsold.index(job)
   print oldjobs[curjobid][0]+"  "+oldjobs[curjobid][3]+"  "+ oldjobs[curjobid][4].split(".")[0]+"  "+oldjob
s[curjobid][2]
   oldjobs_d.append(oldjobs[jidsold.index(job)])

# List the jobs terminated within the last 24 hoursi
 if not args['minimal_out']:
  if len(oldjobs_d) > 0:
   print "-"*cols
   print ""
   newold=[]
   print "  Jobs terminated within the last "+str(d)+" days"
   for job in oldjobs_d:
    if len(job) == 5:

      # Parse something like "2013-12-08 20:26:23.400236"
     last_seen_t=datetime.strptime(" ".join(job[3:5]),"%Y-%m-%d %H:%M:%S.%f" )#%S.%f
     start_t=datetime.strptime(" ".join(job[3:5]),"%Y-%m-%d %H:%M:%S.%f" )#%S.%f
     if (datetime.today()-last_seen_t).days < d: # last d days
      #print job[0],job[3],job[4].split(".")[0],job[2]
      print job[0],last_seen_t.strftime("%b%d/%H:%M"), start_t.strftime("%b%d/%H:%M"), job[2]
      #newold.append(job)
     if (datetime.today()-last_seen_t).days < 14: # keep the last 14 days
      newold.append(job)
     
 outp=open(os.path.expanduser("~"+me) + '/.job_history_p',"w")
 outd=open(os.path.expanduser("~"+me) + '/.job_history_d',"w")

 for job in newjobs: # print present jobs to ~/.job_history_p
  for i in job:
   print>> outp, i,
  print>> outp, ""
 
 try:
  newold
 except:
  newold=oldjobs_d[:]
  
 #print "Saving newjobs to history_d ", newold 
 for job in newold: # print done jobs to ~/.job_history_d
  for i in job:
   print>> outd, i,
  print>> outd, ""
    
# Input/Ouput check (last write X min ago)

if args['io_check']:
 print " "
 print " %3s      %6s       %20s " %("JID", "I/O wait","Last written file")
 for job in newjobs_r:
  last_written=os.popen('ls --full-time -t '+job[2] ).readlines()[1]
  file=job[2]+"/"+ last_written.split()[-1]
  last_time=" ".join(last_written.split()[5:7])
  last_time=last_time.split(".")[0] # trick to get rid of the decimal in the sec
  t1 = datetime.strptime(last_time, '%Y-%m-%d %H:%M:%S')

    # Adjust time of front-end and nodes (if timedelta != 0, Admins of this machine suck)
  current_time=datetime.today() + timedelta(seconds=18) 
  io_waiting=(current_time-t1)#.total_seconds()
  if io_waiting.seconds > 900: # 15 min without writting
   io_waiting=' \033[05;31m '+str(io_waiting).split(".")[0]+' \033[00m '
  elif io_waiting.seconds < 900:
   io_waiting=' \033[05;34m '+str(io_waiting).split(".")[0]+' \033[00m '
  print "%7s %20s  %20s " %(job[0],io_waiting,file)

# 
# Writen by Julen Larrucea
# julen at larrucea dot eu
# http://www.larrucea.eu 

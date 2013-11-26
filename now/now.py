#! /usr/bin/python 

###############################################
#  now: the tool for checking who does what, in the cluster
# Written by Julen Larrucea
# Web: www.larrucea.eu
#
#  It is designed to parse the information from the qstat command. 
#  It might requiere some hacking in order to parse all the variables porperly in your system 
#  
###############################################

 ## #! /usr/bin/python
import os
import sys
from datetime import datetime,timedelta,date


# User friendly cluster usage display
version="0.4"
me=os.popen('echo $USER').read().split()[0]
host=os.popen('echo $HOSTNAME').read().split()[0]
d=1
mm,lc,io=False,False,False

# Parse command line options
if len(sys.argv)>1:
 for i in sys.argv:
  if i.startswith('-'):
   option=i.split('-')[1]
   if option=="d":
    d= sys.argv[sys.argv.index('-d')+1]
    try:
     d=int(d)
     print "yes"
    except:
     print "d must be an integer"
   if option=="lc":
    lc="" 
   if option=="mm":
    mm="" 
   if option=="io":
    io="" 
   if option=="h":
    print '''
  Now help:
   -h: print this help
   -d: number of days to display to display in job history (def. 1, max 7)
   -lc: long columns
   -io: I/O check. Last write to file in directory
   -mm: minimal output
'''
    sys.exit()

timestamp=datetime.today()
timestamp_f=str(timestamp).split(".")[0]

if mm == False :
 print ' \033[05;31mnow v.' + version + ':\033[00m Jobs of user \033[02;36m' + me + ' \033[00m running on \033[02;36m' + host  + '\033[00m  ' + timestamp_f

# Check if ~/.job_history_* files exists (_r running, _q queued, _d done)
oldjobs_r,oldjobs_q,oldjobs_d,newjobs_r,newjobs_q=[],[],[],[],[]
try:
   open(os.path.expanduser("~"+me) + '/.job_history_p')  #previous jobs
   for job in open(os.path.expanduser("~"+me+'/.job_history_p'),"r"):
    if job.split()[1] == "r":
     oldjobs_r.append(job.split("\n")[0].split())
    elif job.split()[1] == "wq" or job.split()[1] == "qw":
     oldjobs_q.append(job.split("\n")[0].split())
   open(os.path.expanduser("~"+me) + '/.job_history_d')
   for job in open(os.path.expanduser("~"+me+'/.job_history_d'),"r"):
    oldjobs_d.append(job.split("\n")[0].split())
except:
   print 'No job history file in home directory: '+ os.path.expanduser("~larrucea")

jobids=os.popen('qstat -u $USER |grep $USER')

def colorst(x):
 if x == "r" or x == "t":
  return '\033[42;34m R \033[0m '
 if x == "t":
  return '\033[42;33m R \033[0m '
 if x == "qw":
  return '\033[47;33m Q \033[0m '
 if x == "rq":
  return '\033[47;31m C \033[0m '
 if x == "hqw":
  return '\033[47;35m H \033[0m '
 if x == "Eqw":
  return '\033[40;31mEqw \033[0m '
 if x == "dr" or x == "dhr":
  return '\033[40;32m C \033[0m '


if lc == False:
 cols=int(os.popen('tput cols').read())-3 #no. of columns in current window
else:
 cols=200


#My running jobs
if mm == False:
 print "%7s  %6s  %6s  %3s %20s " %("Job ID", "#nodes", "User","Status","Path to output")

for i in jobids.readlines():
 np="--"
 jid=i.split()[0]
 name=i.split()[3]
 status=i.split()[4]
 if status=="r":
  np=str(int(i.split()[8])/8)
 try:
  path=os.popen('qstat -j ' + jid + ' |grep cwd').read().split()[1]
 except:
  path=""
 longpath=path
 if len(path) > cols-30:
  path=".../"+"/".join(path.split('/')[5:])
 print "%7s  %3s  %10s  %s %s " %(jid, np, name,colorst(status),path)
 if status == "r" or status == "t":
  newjobs_r.append([jid,status,longpath,timestamp])
 elif status == "qw" or status == "hqw":
  newjobs_q.append([jid,status,longpath,timestamp])

if mm == False:
 print "-"*cols
 print ""

#How many cpus is each active user using
activ_u=[]
for u in os.popen('ls $HOME/../').read().split():
 cpur_u,cpuq_u,cpuh_u=0,0,0
 group_rcpus,group_qcpus=[],[]
 wgroup_rcpus,wgroup_qcpus=[],[]
 if u != "qlaube":
  raw=os.popen('qstat -u ' + u).readlines()
  if len(raw) > 2:
   for j in raw[2:]:
    if j.split()[4] == "r":
     cpur_u=cpur_u + int(j.split()[8])
     if int(j.split()[8]) in group_rcpus:
      wgroup_rcpus[group_rcpus.index(int(j.split()[8]))]+=1
     else:
      group_rcpus.append(int(j.split()[8]))
      wgroup_rcpus.append(1)
    if j.split()[4] == "qw":
     cpuq_u=cpuq_u + int(j.split()[7])
     if int(j.split()[7]) in group_qcpus:
      wgroup_qcpus[group_qcpus.index(int(j.split()[7]))]+=1
     else:
      group_qcpus.append(int(j.split()[7]))
      wgroup_qcpus.append(1)
    if j.split()[4] == "hqw":
     cpuh_u=cpuh_u + int(j.split()[7])
   gr_str_r,gr_str_q="|","|"  #Next lines are for how many jobs per no. of cores
   for i in range(len(group_qcpus)):
    gr_str_q=gr_str_q+ str(wgroup_qcpus[i])+"x"+str(int(group_qcpus[i]/8))+"|"
   for i in range(len(group_rcpus)):
    gr_str_r=gr_str_r+ str(wgroup_rcpus[i])+"x"+str(int(group_rcpus[i]/8))+"|"
   activ_u.append([u,cpur_u/8 , cpuq_u/8, cpuh_u/8,gr_str_r,gr_str_q])


if mm == False:
 if len(activ_u) == 0 and mm == False:
  print "   ## No jobs running at this moment. ##"
  sys.exit()

 #Draw bars with usage
 tot_cpus,tot_queu,tot_hold=0.01,0.01,0.01
 for a in activ_u:
 # tot_cpus=tot_cpus + int(a[1])
  tot_queu=tot_queu + int(a[2])
  tot_hold=tot_hold + int(a[3])

 tot_cpus=272
 tot_nodes=tot_cpus/8

 bars=[]
 for a in activ_u:
  if tot_cpus > 0 and tot_queu > 0 and tot_hold > 0:
   bars.append([int(cols*int(a[1])*1.0/tot_nodes), int(cols*int(a[2])*1.0/tot_queu), int(cols*int(a[3])*1.0/tot_hold)])
 
 for b in range(3): #s= each horizontal bar= [cpus,queu,hold] 
  if b==0:
   colhead="\033["+ str(b+42)+";30mR: "
  if b==1:
   colhead="\033["+ str(b+42)+";30mQ: "
  if b==2:
   colhead="\033["+ str(b+42)+";30mH: "
  print colhead,
  linelength="" 
  for s in range(len(bars)): #b=lines in bars= No. users
   linelength=linelength+"X"*zip(*bars)[b][s]
   sys.stdout.write( "\033["+ str(b+42)+";"+ str(s+35) + "m" +"X"*zip(*bars)[b][s] + "\033[0m"),
  print  "\033["+ str(b+42) +"m" + " "*(cols-len(linelength))+"\033[0m"
 
 if sum(zip(*activ_u)[1]) < tot_cpus:
  print " "+str(sum(zip(*activ_u)[1]))+ "/" +str(tot_nodes)+ " nodes (" + str(sum(zip(*activ_u)[1])*8)+ "/" +str(tot_cpus)+ " cores ) in use. \033[30;42m",str(tot_nodes-sum(zip(*activ_u)[1]))+ " nodes/ "+str(8*(tot_nodes-sum(zip(*activ_u)[1])))+ " cores","Free! \033[0m"  
 else:
  print "All nodes are being used at this moment"
 
 #Print the usage bar
 print ""
 print "      %6s  %8s   %5s   %5s      %8s      %8s " % ( "Nodes", "Running", "Queue", "Hold", "Job/Nodes R","Job/Nodes Q" )
 for s in range(len(bars)):
  print " %22s   %3i   %5i   %5i  %15s %15s " %  ( "\033[0;"+ str(s+35) + "m" + activ_u[s][0] + "\033[0m", activ_u[s][1], activ_u[s][2], activ_u[s][3], activ_u[s][4], activ_u[s][5])
 
 
jidsold,jidsnew=[],[]
newjobs=newjobs_r+newjobs_q
if len(newjobs) > 0:
 for i in zip(*newjobs)[0]:
  jidsnew.append(i)
else:
 jidsnew=[]

oldjobs=oldjobs_r+oldjobs_q
if len(oldjobs) > 0:
 for i in zip(*oldjobs)[0]:
  jidsold.append(i)
else:
  jidsold=[]

if mm == False:
 if len(set(jidsold) - set(jidsnew)) > 0:
  print ""
  print "-"*cols
  print " "
  print " \033[01;31m ### Just finished job(s) !!! ###  \033[00m "
  for job in set(jidsold) - set(jidsnew): # Just finished jobs
   print oldjobs[jidsold.index(job)][0]+"  "+oldjobs[jidsold.index(job)][3]+"  "+ oldjobs[jidsold.index(job)][4].split(".")[0]+"  "+oldjobs[jidsold.index(job)][2]
   oldjobs_d.append(oldjobs[jidsold.index(job)])


 # List the jobs terminated within the last 24 hours
 if len(oldjobs_d) > 0:
  print "-"*cols
  print ""
  newold=[]
  print "  Jobs terminated within the last "+str(24*d)+" hours"
  for job in oldjobs_d:
   if len(job) == 5:
    yy,mm,dd=job[3].split("-")[0],job[3].split("-")[1],job[3].split("-")[2]
    hh,mi,ss=job[4].split(":")[0],job[4].split(":")[1],job[4].split(":")[2].split(".")[0]
    t1=datetime(int(yy),int(mm),int(dd),int(hh),int(mi),int(ss))
    if (datetime.today()-t1).total_seconds() < 86400*d: # last 24 h*d
     print job[0],job[3],job[4].split(".")[0],job[2]
    if (datetime.today()-t1).total_seconds() < 86400*7: # last 24 h*7
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
  
 for job in newold: # print done jobs to ~/.job_history_d
  for i in job:
   print>> outd, i,
  print>> outd, ""
    
# Input/Ouput check (last write X min ago)

if io != False:
 print " "
 print " %3s      %6s       %20s " %("JID", "I/O wait","Last written file")
 for job in newjobs_r:
  last_written=os.popen('ls -lht '+job[2] ).readlines()[1]
  file=job[2]+"/"+ last_written.split()[-1]
  last_time=" ".join(last_written.split()[5:8])
  t1 = datetime.strptime("2013 "+ last_time, '%Y %b %d %H:%M')
  io_waiting=(datetime.today()-t1)#.total_seconds()
  if io_waiting.total_seconds() > 900: # 15 min without writting
   io_waiting=' \033[05;31m '+str(io_waiting).split(".")[0]+' \033[00m '
  elif io_waiting.total_seconds() < 900:
   io_waiting=' \033[05;34m '+str(io_waiting).split(".")[0]+' \033[00m '
  print "%7s %20s  %20s " %(job[0],io_waiting,file)



# 
# Writen by Julen Larrucea
# julen at larrucea dot eu
# http://www.larrucea.eu 

#   This program is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.

#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.

#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#


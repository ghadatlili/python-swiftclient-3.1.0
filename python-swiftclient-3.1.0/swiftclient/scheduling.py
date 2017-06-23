import collections
import os
import math
import time
import swiftclient.functions
import datetime
from time import gmtime, strftime
import datetime
from collections import defaultdict

global iTranslist; iTranslist = []; global transferedThr; transferedThr = []

def assignReqToTs(tsNum, obj, transTime, nbMaxThr, timeline,savedObs,objects):
   global transferedThr;global usedThr_dict; global sched_dict;global nbslots ; global iTranslist;
   assigned = False; exit = False;  exit2 = False; global startt; i = objects.index(obj)
   print ("        *********   tsNum = ", tsNum)
   # print "usedThr_dict[tsNum]", usedThr_dict[tsNum],"deadlines[i]",deadlines[i],"transTime", transTime, "nbMaxThr",nbMaxThr
   # increment_ts : to save the value of tsnum which is the current ts
   t = tsNum ; iTransfered = -1
   while  t < timeline and exit2 == False:
      #print "****************************************  obj", obj
      increment_ts = t ; #print ("objects.index(obj) ",savedObs.index(obj)) ; #print ("int(deadlines[savedObs.index(obj)])", int(deadlines[savedObs.index(obj)])) ;
      #print ("tran",transTime)
      if tsNum  < int(deadlines[savedObs.index(obj)]) - transTime + 1:
            #print "usedThr_dict[increment_ts] ",usedThr_dict[increment_ts]
            while usedThr_dict [increment_ts] < nbMaxThr and increment_ts < t + transTime and exit == False:
               #print "usedThr_dict[increment_ts] = ", usedThr_dict[increment_ts];
               increment_ts = increment_ts + 1 ; #print "tsnum + transTime: ",tsNum + transTime , "increment_ts : ", increment_ts;

            if increment_ts == t + transTime and usedThr_dict [increment_ts] < nbMaxThr and increment_ts - transTime  < int(deadlines[savedObs.index(obj)]) - transTime + 1:
               startt = increment_ts - transTime; #timeslot when the transfer starts
               print ("increment_ts ",increment_ts, "transTime ", transTime);
               if increment_ts - transTime == startt and startt != int(tsNum) and obj.split("_",1)[1] not in iTranslist:
                    iTranslist.append(obj); print ("    ***  starttt ", startt);

               for tc in range(increment_ts - transTime, increment_ts):
                  print ("nbslots[int(savedObs.index(obj))] ; ", nbslots[int(savedObs.index(obj))])
                  print ("savedObs ; ", savedObs)
                  if obj not in sched_dict[tc] and nbslots[int(savedObs.index(obj))] < transTime:
                     sched_dict[tc].append(obj);nbslots[int(savedObs.index(obj))] = nbslots[int(savedObs.index(obj))] + 1;
                     if tc != tsNum: transferedThr[tc] = transferedThr[tc] + 1;
                     usedThr_dict[tc] = usedThr_dict[tc] + 1;
                     if transferedThr[tc] == nbMaxThr: t = t + 1 ;
               assigned = True ;    print ("assigned", assigned) ; exit = True;
            else: t = t + 1 ; #print ("no free slots ");
      else:
          #assigned = False;    print "assigned", assigned ;
          exit2 = True;
   if t == timeline and exit2 == True: assigned = False

   #print "translist   ",iTranslist;
   ''' if len(iTranslist) != 0:
       for x in iTranslist:
         if x in objects:
           #print "x ", x;
           index = objects.index(x);
           #print "objects1 ", objects, objects.count(x)
           #print len(objects), "/", len(deadlines), "/", len(sizes), "/", len(filenames), "/";
           deadlines.pop(index);
           #print deadlines;
           filenames.pop(index);
           #print filenames;
           sizes.pop(index);
           #print sizes;  # Once the request i is scheduled in the current ts, is is removed from the list of objs to schedule
           #print "objects2 ", objects '''
   if tsNum == 4: print ("sched_dict",sched_dict); print ("nbslots ", nbslots) ; print ("usedThr_dict",usedThr_dict);#print "transferedThr : ", transferedThr;
   print ("sched_dict", sched_dict);
   print ("nbslots ", nbslots);
   print ("usedThr_dict", usedThr_dict);  # print
   return nbslots,iTranslist,sched_dict, usedThr_dict, assigned

def removeAndScheduleThenew(obj, obj2,sched_dict):
    exit = False ; i = 0 ; t = 0
    while i < len(sched_dict) :
        v = sched_dict[i]
        if obj in v: v.remove(obj) ; v.append(obj2); i = i + 1 ;#print "obj to reschedule ", obj ; print "obj to schedule ", obj2 ;
        else: i = i + 1 ;


def freethrs (nbTS, nbMaxThr,usedThr_dict): # looking for free threads  to reschedule a request. nbTS ; the required number of transmission times to reschedule
    i = 0 ; j = 0 ; #print "nbTS",nbTS
    while i < len(usedThr_dict) and j < nbTS:
        v = usedThr_dict[i] ; #print v ;
        if v < nbMaxThr:    i = i + 1 ;j = j + 1 ;# print "j",j ;
        else: i = i + 1 ;
        #print "i",i ; print "len(usedThr_dict)",len(usedThr_dict)
    if j == nbTS: ret_value = i - nbTS + 1 ; print ("ind ",ret_value) ;
    else: ret_value = -1 ;
    return ret_value
def switchTS(tsNum,ts, objs, nbslots, nbMaxThr,timeline,sched_dict,usedThr_dict): # object request
    find = False ; find2 = False ; diff = 1000 ; find1 = False; assigned = []; print ("nbslots ;", nbslots) ; i = 0; j = 0; freetimes = []; t = 0 ; obj_to_resched = ""
    print ("**********************      switch ts        ***************");  print  ("sched_dict.values()" , sched_dict)
    print("here",len(nbslots))

    for k in range(len(nbslots)):

                  if nbslots[k] == 0: # it means the obj k was not scheduled in the previous timeslot
                        obj_tosched = objs[k] ;  transTime = 5 ; #transTime = (bw * sizes[objs.index(obj_tosched)])/(nbMaxThr*ts)
                        print (obj_tosched.split('_')[0]) ; max_t_tosched = int(obj_tosched.split('_')[0]) - transTime ; t = tsNum + 1
                        while t >= tsNum + 1 and t <= max_t_tosched and find == False: # look for an object to remove among the placed objs and put in another place
                            print (t) ; v = sched_dict[t] ; j = 0 ; print (v)
                            while j < len(v) and find == False:
                                obj_to_resched = sched_dict[t][j]; print (obj_to_resched)
                                ind_free = freethrs(nbslots[objs.index(obj_to_resched)], nbMaxThr, usedThr_dict); print (ind_free) ;
                                if ind_free != -1:
                                    if int(obj_to_resched.split('_')[0]) >= ind_free + nbslots[objs.index(obj_to_resched)]:
                                        find = True
                                        removeAndScheduleThenew(obj_to_resched, obj_tosched, sched_dict);
                                        print ("sched_dict", sched_dict); print ("usedThr_dict", usedThr_dict)
                                        for s in range(ind_free , ind_free  + nbslots[objs.index(obj_to_resched)]):
                                            sched_dict[s].append(obj_to_resched) ; usedThr_dict[s] = usedThr_dict[s] + 1 # reschedule the existing obj
                                    else: j = j + 1;
                                else: find = True;
                            t = t + 1
                        print ("sched_dict",sched_dict) ; print ("usedThr_dict",usedThr_dict)
                  #else:print ("no obj to reschedule")
    '''for i in range(len(objects)):
        if nbslots[i] >= nbslots[req]:
            if diff > nbslots[i] - nbslots[req]:
                diff = nbslots[i] - nbslots[req]; bSwitch = i ; find1 = True ;# bSwitch is the best request to switch with
    if find1 == True :
        lessusedThr = 1000;
        for t in range(timeline):
            if deadlines[bSwitch] >= t + nbslots[bSwitch]:
                if lessusedThr > usedThr_dict[t]:
                    lessusedThr = usedThr_dict[t] ; lessbusytime = t ; find2 = True;'''
    return sched_dict

def checkRequestScheduled (objs):
     global sched_dict ; nbslots = [0] * len(objs) ; assigned = [] ; i = 0 ; j = 0 ; #print  "sched_dict.values()" ,  sched_dict.values()
     k = 0
     for k in range(len(objs)):
        exit2 = False; exit = False;
        while i < len(sched_dict) and exit2 == False:
              v = sched_dict[i]
              while j < len(v) and exit == False:
                  if objs[k] == v[j]: assigned.append(True) ; nbslots[k] = nbslots[k] + 1 ; exit = True ;
                  else : j = j + 1 ;
              if j == len(v) or exit == True :  i = i + 1 ; j = 0 ; exit = False
        if i == len(sched_dict): assigned.append(False); i = 0 ; j = 0 ; exit2 = True ;
     return nbslots;

def schedule_requests(algoChoice, timeline,objects): # timeline is the number of timeslots
   ts = swiftclient.functions.tscalcul() ;
   global countNotAssigned1 ; countNotAssigned1 = 0  ; global savedObs;
   global transferedThr ; transferedThr = []; global nbslots ; nbslots  = [0] * 50 ; global iTransList; global rejR ; rejR = []
   global sched_dict;sched_dict = defaultdict(list) ; global rejR ;rejR = [];  global usedThr_dict ; usedThr_dict = {} ; usedThr_dict = defaultdict(lambda:0,usedThr_dict)
   now = datetime.datetime.now()
   print(datetime.datetime.now())
   end=now+datetime.timedelta(seconds=timeline)
   print (end); tsNum = 0 ; print ("timeline = ", timeline)
   l=[] ;l.append(now); objects2 = ["4_/home/AN28060/Desktop/d4 (copy).txt", "5_/home/AN28060/Desktop/d5 (copy).txt",
                       "4_/home/AN28060/Desktop/d42 (copy).txt", "4_/home/AN28060/Desktop/d43 (copy).txt",
                       "5_/home/AN28060/Desktop/d51 (copy).txt", "6_/home/AN28060/Desktop/d6 (copy).txt",
                       "6_/home/AN28060/Desktop/d61 (copy).txt", "7_/home/AN28060/Desktop/d7 (copy).txt",
                       "7_/home/AN28060/Desktop/d71 (copy).txt", "8_/home/AN28060/Desktop/d8 (copy).txt",
                       "8_/home/AN28060/Desktop/d81 (copy).txt", "8_/home/AN28060/Desktop/d82 (copy).txt"]
   while now<=end and tsNum < timeline:
      now += datetime.timedelta(seconds=ts);
      l.append(now);
      if l[tsNum + 1] == l[tsNum] + datetime.timedelta(seconds=ts):
          tsNum = tsNum + 1 ;
          print ("      tsnum :", tsNum);   sched_dict,objs,iTransList = schedule_requests_per_ts(algoChoice,tsNum, ts, nbMaxThr,timeline,objects);
          if tsNum == 4: iTransList.extend(objects2);objs = iTransList ; print ("objects = ",objects)
          #else: objs = objects ;
          if algoChoice==1:
              nbslots = checkRequestScheduled(objs); print ("nbslots ",nbslots) ; print ("objs",objs);
              sched_dict = switchTS(tsNum, ts, objs, nbslots,nbMaxThr, timeline, sched_dict, usedThr_dict) ; time.sleep(ts) ;

   return sched_dict

   #print [t.strftime("%H:%M") for t in l]

def schedule_requests_per_ts(algoChoice,tsNum, ts, nbMaxThr,timeline,objects):
   global transferedThr ; global usedThr_dict; global sched_dict; global nbslots ;  global iTranslist; global rejR; global countNotAssigned1; global savedObs;
   #print "length = ", len(objects)
   nbslots = [0] * 1200; global avail_bw ;
   global deadlines ; deadlines = [] ; global filenames;filenames = [] ; l = [] ; x = [] ;
   if tsNum > 1: objects = iTranslist; print ("iiiiiiiiiiiiiiiiiiiTranslist : ", iTranslist, "len(iTranslist)", len(iTranslist));
   for i in range(len(objects)):
      s = False
      if "678B" in objects[i] or objects[i].split('/',5)[0] == "678B": deadlines.append(1) ; s = True;#
      else:
          if "15M" in objects[i] or objects[i].split('/')[0] == "15M": deadlines.append(5); s = True;
          else:
            if "403K" in objects[i] or objects[i].split('/',5)[0] == "403K": deadlines.append(3) ;s = True;
            else:
              if "26K" in objects[i] or objects[i].split('/')[0] == "26K": deadlines.append(2) ;s = True;
              else:
                if "2500M" in objects[i] or objects[i].split('/')[0] == "2500M": deadlines.append(500) ;s = True;
                else:
                  if "203M" in objects[i] or objects[i].split('/')[0] == "203M": deadlines.append(30);s = True;
                  else : deadlines.append((objects[i]).split('_')[0]) ; filenames.append((objects[i]).split('_')[1])

      if s == True and len(objects[i].split('_')) != 3: filenames.append(objects[i]) ;
      if s == True and len(objects[i].split('_')) == 3 : filenames.append(objects[i].split('_')[1]+"_"+objects[i].split('_')[2]);  print ("filenames",filenames)
      if deadlines[i] not in swiftclient.shell.down_dict[objects[i]]:swiftclient.shell.down_dict[objects[i]].append(deadlines[i])
      x.append(deadlines[i]); x.append(filenames[i]);l.append(x);x = [] ;
   ts = swiftclient.functions.tscalcul() #seconds
   #obj_dict = dict()
   d1 = defaultdict(list)

   for k, v in l:
       d1[k].append(v)
   obj_dict = dict((k, tuple(v)) for k, v in d1.items()) #print "obj_dict  : ", obj_dict
   #obj_dict = defaultdict(list) ; #obj_dict = {int(deadlines[i])/ts: objects[i] for i in range(len(objects))}
   od = collections.OrderedDict(sorted(obj_dict.items())) #  print "od", len(od) ; print "od items", od.items()
   objects = []
   for k, v in od.items():
      for i in range(len(v)):
          objects.append(v[i])

   deadlines.sort(key=int) # print "deadlines",deadlines ; print "objects",objects ; print "ob", len(objects);
   filenames = objects ; objects = []

   for i in range(len(filenames)):
       ob = str(deadlines[i])+"_"+filenames[i]; objects.append(ob);

   global sizes; sizes = []; #print ("objects",objects) ; print ("filenames",filenames)
   for i in range(len(objects)):
      sizes.append(os.path.getsize("/home/AN28060/WorkSpace/Scripts/"+filenames[i])) ; #print ("***************sizes",sizes);  # return size in bytes
      if sizes[i] not in swiftclient.shell.down_dict[filenames[i]]:swiftclient.shell.down_dict[filenames[i]].append(sizes[i])

   bw = swiftclient.functions.bandwidth_monitoring()
   global starttime;  starttime = []; #print "  --------   objectss = ",objects
   i = 0 ;
   savedObs= objects;
   for obj in objects:
      #print ("obj",obj) ; #  print "len(objects)", len(objects) ; print "dead", deadlines[i] ; print "ts+trans", ts + transTime ;
       #transTime = (bw * sizes[i])/(nbMaxThr*ts)
      print ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaavail_bw", swiftclient.service.avail_bw)
      if tsNum > 1 :
          if swiftclient.service.avail_bw  == 0.0: transTime = int(math.ceil((swiftclient.shell.down_dict[obj.split("_",1)[1]][1]) / (75000000 * swiftclient.functions.tscalcul()) ))+1;

          else:transTime = int(math.ceil(( swiftclient.shell.down_dict[obj.split("_",1)[1]][1]) / (swiftclient.service.avail_bw * swiftclient.functions.tscalcul() )));

      else:
          transTime = int(math.ceil((swiftclient.shell.down_dict[obj.split("_",1)[1]][1]) / (75000000 * swiftclient.functions.tscalcul()) ));
      print("transTime ", transTime)
      if obj == "8_/home/AN28060/Desktop/d81.txt": transTime = 6;

      print ("Request assignment");
      if i == 0 and tsNum == 1:
          sched_dict = defaultdict(list) ; usedThr_dict = defaultdict(int) ; transferedThr = defaultdict(int) ;
          for t in range(1,timeline): usedThr_dict[t] = 0 ;
      '''  if i != 0 and tsNum == 1:
          (iTranslist, sched_dict, transferedThr, assigned1) = assignReqToTs(tsNum, obj, transTime, nbMaxThr, timeline,
                                                                             sched_dict, usedThr_dict, savedObs)
      if i == 0 and tsNum == 2:
          (iTranslist, sched_dict, transferedThr, assigned1) = assignReqToTs(tsNum, obj, transTime, nbMaxThr, timeline,
                                                                             sched_dict, usedThr_dict, savedObs)'''

      if tsNum > 1:
          print("obj : ", obj);print ("itrans : ",iTranslist)
          if obj.split("_",1)[1] in iTranslist:
              print ("obj : ",obj)
              #print "iTranslist ", iTranslist;
              (nbslots,iTranslist, sched_dict, usedThr_dict, assigned1) = assignReqToTs(tsNum, obj,transTime,nbMaxThr, timeline, savedObs,objects)
              #print "transferedThr :", transferedThr ; #print "iTranslist : ", iTranslist;
      else: (nbslots,iTranslist, sched_dict, usedThr_dict, assigned1) = assignReqToTs(tsNum, obj, transTime, nbMaxThr, timeline,  savedObs,objects)
          #print "transferedThr :", transferedThr ; #print "iTranslist : ", iTranslist;

      #print ("objects",objects)


      i = i + 1; print ("i : ",i);

      if len(objects) == i:
          print (" *****  break  *******");

   return sched_dict,objects,iTranslist;
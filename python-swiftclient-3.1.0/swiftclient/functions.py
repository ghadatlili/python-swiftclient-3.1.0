def tscalcul():
   ts = 1
   return ts
def bandwidth_monitoring():
   bw = 3
   return bw # Mbits/s


'''def assignReqToTs(tsNum, i,transTime,nbMaxThr):
   assigned = False; exit = False; global startt;
   print "        *********   tsNum = ", tsNum # print "usedThr_dict[tsNum]", usedThr_dict[tsNum],"deadlines[i]",deadlines[i],"transTime", transTime, "nbMaxThr",nbMaxThr
   increment_ts = tsNum # increment_ts : to save the value of tsnum which is the current ts
   while  usedThr_dict[tsNum] < nbMaxThr and exit == False:
      if tsNum  < int(deadlines[i]) - transTime + 1:
         increment_ts = increment_ts + 1 ; #print "tsnum + transTime: ",tsNum + transTime , "increment_ts : ", increment_ts;
         if increment_ts == tsNum + transTime:
           startt = transTime; #timeslot when the transfer starts
           for t in range(increment_ts - transTime, transTime + 1):
             sched_dict[t].append(i);
             #if t == tsNum : objects.pop(i);deadlines.pop(i); sizes.pop(i); filenames.pop(i);#Once the request i is scheduled in the current ts, is is removed from the list of objs to schedule
             usedThr_dict[t] = usedThr_dict[t] + 1
           assigned = True ; exit = True;
      else: assigned = False; exit = True;
      #if usedThr_dict[tsNum] == nbMaxThr: 
   print "sched_dict",sched_dict; print "usedThr_dict",usedThr_dict;
   return  usedThr_dict, assigned'''


# at the end of the file
'''for i in range(len(objects)):
   print "i", i;  # print "len(objects)", len(objects) ; print "dead", deadlines[i] ; print "ts+trans", ts + transTime ;
   transTime = 3;  # transTime = (bw * sizes[i])/(nbMaxThr*ts)
   nbslots.append(transTime);
   print "  trans = ", transTime;
   # if deadlines[i] < ts + transTime:
   # objects.pop(i); deadlines.pop(i); sizes.pop(i);filenames.pop(i);rejR.append(i); print "Request rejected because it cannot meet its deadline"
   # else:
   print "Request assignment";
   if i == 0:
      sched_dict = defaultdict(list)
      usedThr_dict = defaultdict(int)
   (iTranslist, sched_dict, transferedThr, assigned1) = assignReqToTs(tsNum, i, transTime, nbMaxThr, timeline,
                                                                      sched_dict, usedThr_dict)
   print "transferedThr :", transferedThr

   if i == len(objects) - 1:
      break'''
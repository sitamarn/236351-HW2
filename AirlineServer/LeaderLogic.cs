using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AirlineServer
{
    class LeaderLogic
    {
        public LeaderLogic()
        {

        }


        public void respondIfMachineIsDead(){
            if (checkIfLeader())
            {
                Tuple<Uri, Uri> owners = chooseBackupAndPrimaryConsideringLoadBalancing(null);


            }
        }
        private Tuple<Uri, Uri> chooseBackupAndPrimaryConsideringLoadBalancing(Dictionary<string, Tuple<Uri, Uri>> balanceSnapshot)
        {
                Dictionary<Uri, int> machinesAndLoad = new Dictionary<Uri, int>();
                foreach (string seller in balanceSnapshot.Keys)
                {
                    if (!machinesAndLoad.Keys.Contains(balanceSnapshot[seller].Item1))
                    {
                        machinesAndLoad.Add(balanceSnapshot[seller].Item1, 1);
                    }
                    else
                    {
                        machinesAndLoad[balanceSnapshot[seller].Item1]++;
                    }

                    if (!machinesAndLoad.Keys.Contains(balanceSnapshot[seller].Item2))
                    {
                        machinesAndLoad.Add(balanceSnapshot[seller].Item2, 1);
                    }
                    else
                    {
                        machinesAndLoad[balanceSnapshot[seller].Item2]++;
                    }
                }

                int minMultplicity = machinesAndLoad.Values.Min();

                Uri lightMachinP = (from p in machinesAndLoad where p.Value == minMultplicity select p).Max().Key;

                //return new Uri(lightMachineP, lightMachineB);
                return null;
            
        }

        private bool checkIfLeader()
        {
            string minMachine = TreeViewLib.TreeView.Instance.Machines.Keys.Min();
            //string myID = TreeViewLib.TreeView.Instance.myID;
            //return myID.Equals(minMachine);
            return true;
        }
    }
}

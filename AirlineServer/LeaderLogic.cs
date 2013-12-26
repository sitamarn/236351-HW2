using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TreeViewLib;

namespace AirlineServer
{
    class LeaderLogic
    {
        Boolean isLeader = false;
        public LeaderLogic()
        {

        }

        public void balanceTheTree(string coordinator, Uri machine)
        {
            Dictionary<string, ZNodesDataStructures.MachineNode> machines = Machines();
            Dictionary<string, int> balancesP = new Dictionary<string, int>();
            Dictionary<string, int> balancesB = new Dictionary<string, int>();
            int averageP = 0, averageB = 0;
            foreach (string mac in machines.Keys)
            {
                if (mac.Equals(coordinator)) continue;
                averageP += machines[mac].primaryOf.Count;
                averageB += machines[mac].backsUp.Count;
            }
            averageP = Convert.ToInt32(Math.Floor(Convert.ToDecimal(averageP) / machines.Count));
            averageB = Convert.ToInt32(Math.Floor(Convert.ToDecimal(averageB) / machines.Count));
            int counter;
            for (int i = 1; i < averageP; i++)
            {

            }
            foreach (string mac in machines.Keys)
            {
                if (mac.Equals(coordinator)) continue;


            }

        }

        public void respondIfNewNode(String sellerName, Uri machine)
        {
            if(isLeader){
                Dictionary<string, ZNodesDataStructures.MachineNode> machines =  Machines();
                


            }
            if(sellersWhoLostPrimary.Intersect(pri
            if (checkIfLeader())
            {
                Tuple<Uri, Uri> owners = chooseBackupAndPrimaryConsideringLoadBalancing(null);


            }
        }

        public void respondIfMachineIsDead(List<String> sellersWhoLostPrimary, List<String> sellersWhoLostBackup)
        {
            if(sellersWhoLostPrimary.Intersect(pri
            if (checkIfLeader())
            {
                Tuple<Uri, Uri> owners = chooseBackupAndPrimaryConsideringLoadBalancing(null);


            }
        }
        public void respondIfMachineIsDead(){
            
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

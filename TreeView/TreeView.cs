using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TreeViewLib
{
    public class TreeView : ITreeView
    {
        public class ChildrenDiff
        {
            public List<String> added   = new List<String>();
            public List<String> dropped = new List<String>();
        }

        private ZooKeeperWrapper zk = null;

        private String machinesPath = null;
        private String sellersPath = null;

        private List<String> machinesNames = new List<String>();
        private List<String> sellersNames = new List<String>();

        private Dictionary<String, ZNodesDataStructures.MachineNode> machinesData = new Dictionary<string,ZNodesDataStructures.MachineNode>();
        public Dictionary<String, ZNodesDataStructures.MachineNode> Machines
        {
            get { return machinesData; }
        }


        public List<String> MachinesNames
        {
            get { return machinesNames; }
        }

        public List<String> SellersNames
        {
            get { return sellersNames; }
        }

        public TreeView(ZooKeeperWrapper zk, String machinesPath, String sellersPath)
        {
            this.machinesPath = machinesPath;
            this.sellersPath = sellersPath;
            this.zk = zk;
        }

        public void refresh() 
        {
            var newMachinesView = zk.GetChildren(machinesPath, false);
            var newSellersView = zk.GetChildren(sellersPath, false);

            machinesNames.Clear();
            machinesNames.AddRange(newMachinesView);

            sellersNames.Clear();
            sellersNames.AddRange(newSellersView);

            machinesData.Clear();
            machinesNames = machinesNames.Where(delegate(string elm, int index)
            {
                bool predict = false;
                try
                {
                    var elmPath = machinesPath + "/" + elm;
                    if (zk.Exists(elmPath))
                    {
                        ZNodesDataStructures.MachineNode data = zk.GetData<ZNodesDataStructures.MachineNode>(elmPath,false);
                        machinesData.Add(elm, data);
                        predict = true;
                    }
                }
                catch (Exception ex) { Console.WriteLine(elm + " was filtered from machiens because " + ex.Message); }
                return predict;
            }).ToList();
        }

        public ChildrenDiff update()
        {
            ChildrenDiff cd = new ChildrenDiff();
            var newMachinesView = zk.GetChildren(machinesPath, false);
            //List<String> stillAlive = newMachinesView.Intersect(machinesNames).ToList();
            cd.added = newMachinesView.Except(machinesNames).ToList();
            cd.dropped = machinesNames.Except(newMachinesView).ToList();

            //Console.WriteLine("Old machines: " + String.Join(" ",machinesNames));
            //Console.WriteLine("New machines: " + String.Join(" ", newMachinesView));
            //Console.WriteLine("Considered dropped: " + String.Join(" ", cd.dropped));
            //Console.WriteLine("Considered added: " + String.Join(" ", cd.added));

            foreach (var dropped in cd.dropped)
            {
                machinesData.Remove(dropped);
            }

            updateMachineDataSelectively(cd.added);

            machinesNames = newMachinesView;
            return cd;
        }

        public void removeMachine(string machineName)
        {
            if(machinesNames.Contains(machineName)) 
            {
                machinesNames.Remove(machineName);
                machinesData.Remove(machineName);
            }
        }

        public ZNodesDataStructures.MachineNode getLocalMachineData(string machineName)
        {
            ZNodesDataStructures.MachineNode node = null;
            if (machinesData.ContainsKey(machineName))
            {
                node = machinesData[machineName];
            }

            return node;
        }

        private void updateMachineDataSelectively(List<String> machines)
        {
            foreach(var machine in machines) 
            {
                updateMachineDataSelectively(machine);
            }
        }

        private void updateMachineDataSelectively(String machineName) 
        {
            try
            {
                var elmPath = machinesPath + "/" + machineName;
                if (zk.Exists(elmPath))
                {
                    ZNodesDataStructures.MachineNode data = zk.GetData<ZNodesDataStructures.MachineNode>(elmPath, false);
                    machinesData.Add(machineName, data);
                }
            }
            catch (Exception) { Console.WriteLine("Failed fetching " + machineName + " data, ignoring update request"); }
        }

        /// <summary>
        /// Shows current cluster Group View, calling this method will update the state of the tree view 
        /// with latest view
        /// </summary>
        /// <returns></returns>
        public override String ToString()
        {
            StringBuilder sb = new StringBuilder();
            refresh();

            sb.AppendLine("Machines: " + String.Join(" ", machinesNames.ToArray()));
//            sb.AppendLine("Machines Data: ");
//            sb.AppendLine(String.Join("\n", machinesData.ToArray()));
            sb.AppendLine("Sellers: " + String.Join(" ", sellersNames.ToArray()));

            return sb.ToString();
        }
    }
}

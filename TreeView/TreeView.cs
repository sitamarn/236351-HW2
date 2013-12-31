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
        public Dictionary<String, ZNodesDataStructures.MachineNode> Machines // Machines by their node name
        {
            get { return machinesData; }
        }


        public List<String> MachinesNames
        {
            get { return new List<String>(machinesNames); }
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

        // Prints the tree without changing the tree view state
        public static String PrintTree(ZooKeeperWrapper zk, String machinesPath, String sellersPath, String stamp)
        {
            StringBuilder sb = new StringBuilder();
            List<String> machines = zk.GetChildren(machinesPath, true);
            sb.AppendLine("["+stamp+"] Machines Tree: ");
            foreach (var machine in machines)
            {
                var machineRecord = zk.GetData<ZNodesDataStructures.MachineNode>(machinesPath + "/" + machine, true);
                sb.AppendLine("["+stamp+"] \\-" + machine);
                sb.AppendLine("["+stamp+"]       \\-P: " + String.Join(" ", machineRecord.primaryOf));
                sb.AppendLine("["+stamp+"]       \\-B: " + String.Join(" ", machineRecord.backsUp)); 
            }
            sb.AppendLine("[" + stamp + "] Sellers Tree: ");
            List<String> sellers = zk.GetChildren(sellersPath, true);
            foreach (var seller in sellers)
            {
                sb.AppendLine("["+stamp+"] \\-" + seller);
                var sellerMachines = zk.GetChildren(sellersPath + "/" + seller,true);
                foreach (var sellerMachine in sellerMachines)
                {
                    var machineSellerRecord = zk.GetData<ZNodesDataStructures.SellerNode>(sellersPath + "/" + seller + "/" + sellerMachine,true);
                    sb.AppendLine("[" + stamp + "]       \\-"+machineSellerRecord.role+": " + sellerMachine+ " " + machineSellerRecord.uri);
                }
            }
            return sb.ToString();
        }

        public ChildrenDiff update()
        {
            ChildrenDiff cd = new ChildrenDiff();
            var newMachinesView = zk.GetChildren(machinesPath, false);
            //List<String> stillAlive = newMachinesView.Intersect(machinesNames).ToList();
            cd.added = newMachinesView.Except(machinesNames).ToList();
            cd.dropped = machinesNames.Except(newMachinesView).ToList();

            Console.WriteLine("Old machines: " + String.Join(" ", machinesNames));
            Console.WriteLine("New machines: " + String.Join(" ", newMachinesView));
            Console.WriteLine("Considered dropped: " + String.Join(" ", cd.dropped));
            Console.WriteLine("Considered added: " + String.Join(" ", cd.added));

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

        /// <summary>
        /// Updates machineName data record from zookeeper and returns the old data which was overwritten
        /// </summary>
        /// <param name="machineName"></param>
        /// <returns></returns>
        public ZNodesDataStructures.MachineNode updateMachineData(String machineName)
        {
            ZNodesDataStructures.MachineNode oldData = getLocalMachineData(machineName);
            updateMachineDataSelectively(machineName);
            return oldData;
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
                    if(machinesData.ContainsKey(machineName)) 
                    {
                        machinesData[machineName] = data;
                    } 
                    else 
                    {
                        machinesData.Add(machineName, data);
                    }
                }
            }
            catch (Exception ex) 
            { 
                Console.WriteLine("Failed fetching " + machineName + " data, ignoring update request");
                Console.WriteLine(ex.Message);
            }
        }

        /// <summary>
        /// Shows current cluster Group View, calling this method will update the state of the tree view 
        /// with latest view
        /// </summary>
        /// <returns></returns>
        public override String ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine("=============================================================");
            sb.AppendLine("Local tree view: ");
            foreach (var record in machinesData)
            {
                var machineName = record.Key;
                var machineRecord = record.Value;
                sb.AppendLine(machineName);
                sb.AppendLine("   \\- P: " + String.Join(" ", machineRecord.primaryOf));
                sb.AppendLine("   \\- B: " + String.Join(" ", machineRecord.backsUp));
                //Console.WriteLine(record.Key + " - " record.Value.);
                //record.Key
            }
            

            //List<String> machines = zk.GetChildren(machinesPath, false);
            //sb.AppendLine("Machines: " + String.Join(" ", machines.ToArray()));

            //List<String> sellers = zk.GetChildren(sellersPath, false);
            //foreach (var seller in sellers)
            //{
            //    sb.Append("Seller " + seller + ":");
            //    sb.AppendLine("{" + String.Join(",", zk.GetChildren(sellersPath + "/" + seller, false)) + "}");
            //}



            //sb.AppendLine("Remote tree view: ");
            //sb.AppendLine("Machines: " + String.Join(" ", machinesNames.ToArray()));
            //foreach (var seller in sellersNames)
            //{
            //    sb.Append("Seller " + seller + ":"); 
            //    sb.AppendLine("["+String.Join(",", zk.GetChildren(sellersPath + "/" + seller, false))+"]");
            //}
            return sb.ToString();
        }
    }
}

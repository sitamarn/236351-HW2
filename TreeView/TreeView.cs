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

        private Dictionary<String, Dictionary<ZNodesDataStructures.SellerNode.NodeRole, ZNodesDataStructures.SellerNode>> sellersData =
            new Dictionary<string, Dictionary<ZNodesDataStructures.SellerNode.NodeRole, ZNodesDataStructures.SellerNode>>();

        public Dictionary<String, Dictionary<ZNodesDataStructures.SellerNode.NodeRole, ZNodesDataStructures.SellerNode>> Sellers // Machines by their node name
        {
            get { return sellersData; }
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

            var newMachinesView = zk.GetChildren(machinesPath, false);
            var newSellersView = zk.GetChildren(sellersPath, false);

            // Build machines tree from ZK
            machinesData.Clear();
            foreach (var machine in newMachinesView)
            {
                var machineData = zk.GetData<ZNodesDataStructures.MachineNode>(machinesPath + "/" + machine, false);
                machinesData.Add(machine, machineData);
            }

            // Build sellers tree from ZK
            sellersData.Clear();
            foreach (var seller in newSellersView)
            {
                var sellerMachines = zk.GetChildren(sellersPath + "/" + seller, false);
                if (sellerMachines.Count > 2)
                {
                    throw new Exception("Seller " + seller + " has too many machines: " + String.Join(" ", sellerMachines));
                }

                Dictionary<ZNodesDataStructures.SellerNode.NodeRole,ZNodesDataStructures.SellerNode> nodeRoles = new Dictionary<ZNodesDataStructures.SellerNode.NodeRole,ZNodesDataStructures.SellerNode>();
                foreach(var machine in sellerMachines) {
                    var machineSellerNode = zk.GetData <ZNodesDataStructures.SellerNode>(sellersPath + "/" + seller + "/" + machine, false);
                    nodeRoles[machineSellerNode.role] = machineSellerNode;
                }

                //if (nodeRoles.Count != 2)
                //{
                //    throw new Exception("Seller " + seller + "doesn't have needed roles " + String.Join(" ", nodeRoles.Keys.ToArray()));
                //}

                sellersData[seller] = new Dictionary<ZNodesDataStructures.SellerNode.NodeRole, ZNodesDataStructures.SellerNode>(nodeRoles);
            }
        }

        public void SetPrimaryOf(String seller, ZNodesDataStructures.SellerNode newPrimary)
        {
            if (sellersData.ContainsKey(seller))
            {
                if (sellersData[seller].ContainsKey(ZNodesDataStructures.SellerNode.NodeRole.Main))
                {
                    sellersData[seller][ZNodesDataStructures.SellerNode.NodeRole.Main] = newPrimary;
                }
                else
                {
                    sellersData[seller].Add(ZNodesDataStructures.SellerNode.NodeRole.Main,newPrimary);
                }
            }
        }

        public void SetBackupOf(String seller, ZNodesDataStructures.SellerNode newBackup)
        {
            if (sellersData.ContainsKey(seller))
            {
                if (sellersData[seller].ContainsKey(ZNodesDataStructures.SellerNode.NodeRole.Backup))
                {
                    sellersData[seller][ZNodesDataStructures.SellerNode.NodeRole.Backup] = newBackup;
                }
                else
                {
                    sellersData[seller].Add(ZNodesDataStructures.SellerNode.NodeRole.Backup, newBackup);
                }
            }
        }

        public void RemoveBackupOf(String seller, String machineName)
        {
            if (sellersData.ContainsKey(seller))
            {
                if (sellersData[seller].ContainsKey(ZNodesDataStructures.SellerNode.NodeRole.Backup))
                {
                    var machineRecord = sellersData[seller][ZNodesDataStructures.SellerNode.NodeRole.Backup];
                    if (machineRecord != null)
                    {
                        if (machineRecord.machineName != machineName)
                        {
                            throw new Exception("Seller " + seller + " doesn't have " + machineName + " as backup, instead it has " + machineRecord.machineName);
                        }
                    }
                    sellersData[seller][ZNodesDataStructures.SellerNode.NodeRole.Backup] = null;
                }
            }
        }

        public void RemovePrimaryOf(String seller, String machineName)
        {
            if (sellersData.ContainsKey(seller))
            {
                if (sellersData[seller].ContainsKey(ZNodesDataStructures.SellerNode.NodeRole.Main))
                {
                    var machineRecord = sellersData[seller][ZNodesDataStructures.SellerNode.NodeRole.Main];
                    if (machineRecord != null)
                    {
                        if (machineRecord.machineName != machineName)
                        {
                            throw new Exception("Seller " + seller + " doesn't have " + machineName + " as backup, instead it has " + machineRecord.machineName);
                        }
                    }
                    sellersData[seller][ZNodesDataStructures.SellerNode.NodeRole.Main] = null;
                }
            }
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


        public ZNodesDataStructures.MachineNode updateMachine(String machineName, ZNodesDataStructures.MachineNode newData)
        {
            ZNodesDataStructures.MachineNode oldData = null;
            if (!machinesData.ContainsKey(machineName))
            {
                Console.WriteLine("*WARNING* Trying to update machine " + machineName + " which isn't contained in local view");
            }
            if (machinesData.ContainsKey(machineName))
            {
                oldData = machinesData[machineName];
                machinesData[machineName] = newData;
            }
            return oldData;
        }

        public void addMachine(String machineName, ZNodesDataStructures.MachineNode newData)
        {
            if (machinesData.ContainsKey(machineName))
            {
                Console.WriteLine("*WARNING* Trying to add machine " + machineName + " which is already contained in local view");
                machinesData[machineName] = newData;
            }
            else
            {
                machinesData.Add(machineName, newData);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="machineName"></param>
        public void removeMachine(string machineName)
        {

           
            if(!machinesData.Remove(machineName))       
            {
                Console.WriteLine("*WARNING* Trying to remove machine " + machineName + " which doesn't exist in local view");
            }
        }


        // 
        public ChildrenDiff update(Dictionary<String, ZNodesDataStructures.MachineNode> newSnapshot)
        {
            ChildrenDiff cd = new ChildrenDiff();


            return cd;
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
            }
            return sb.ToString();
        }
    }
}

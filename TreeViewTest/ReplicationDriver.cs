using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TreeViewLib;
using ZooKeeperNet;

namespace TreeViewTest
{
    class ReplicationDriver
    {
        String cluster = "";
        ZooKeeperWrapper zk = new ZooKeeperWrapper("localhost", 10000, 5, null);

        public String MachinesPath
        {
            get { return "/" + cluster + "/" + AirlineReplicationModule.MACHINES_NODE; }
        }

        public String SellersPath
        {
            get { return "/" + cluster + "/" + AirlineReplicationModule.SELLERS_NODE; }
        }

        public String BarrierPath
        {
            get { return "/" + cluster + "/" + AirlineReplicationModule.BARRIER_NODE; }
        }

        public ReplicationDriver(String cluster) 
        {
            this.cluster = cluster;        
        }

        public void purgeZooKeeper()
        {
            List<String> subtrees = new List<String>() { MachinesPath, SellersPath, BarrierPath };
            foreach (var subtree in subtrees)
            {
                //Console.WriteLine("Checking if " + subtree + " subtree exists");
                if (zk.Exists(subtree))
                {
                    List<String> children = zk.GetChildren(subtree, false);
                    foreach (var child in children)
                    {
                        try
                        {
                            String subpath = subtree + "/" + child;
                            List<String> subchildren = zk.GetChildren(subpath, false);
                            foreach (var subchild in subchildren)
                            {
                                zk.Delete(subpath + "/" + subchild);
                            }
                            //Console.WriteLine("Trying to remove " + subtree + "/" + child);
                            zk.Delete(subtree + "/" + child);
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("Failed removing child " + subtree + "/" + child);
                            Console.WriteLine(e.Message);
                        }
                    }
                    try
                    {
                        zk.Delete(subtree);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Failed deleting node " + subtree);
                        Console.WriteLine(ex.Message);
                    }
                }
            }
        }

        public void addMachine(String machineName, ZNodesDataStructures.MachineNode node)
        {
            zk.Create(MachinesPath + "/" + machineName, node, Ids.OPEN_ACL_UNSAFE, CreateMode.Ephemeral);
        }

        public void addSeller(String sellerName)
        {
            zk.Create(SellersPath + "/" + sellerName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
        }

        public void updateMachine(String machineName, ZNodesDataStructures.MachineNode node)
        {
            zk.SetData(MachinesPath + "/" + machineName, node);
        }

        public ZNodesDataStructures.MachineNode getMachineRecord(String machineName)
        {
            return zk.GetData<ZNodesDataStructures.MachineNode>(MachinesPath + "/" + machineName, false);
        }

        public void setMachineAsSellerOf(String machineName, String sellerName, ZNodesDataStructures.SellerNode.NodeRole machineRole)
        {
            ZNodesDataStructures.MachineNode data = getMachineRecord(machineName);

            ZNodesDataStructures.SellerNode sellerData = new ZNodesDataStructures.SellerNode() 
            { nodeId = MachinesPath + "/" + machineName, role = machineRole, uri = data.uri, version = -1 };

            zk.Create(SellersPath + "/" + sellerName + "/" + machineName, sellerData, Ids.OPEN_ACL_UNSAFE, CreateMode.Ephemeral);
        }

    }
}

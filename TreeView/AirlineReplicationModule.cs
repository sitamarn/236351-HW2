using Org.Apache.Zookeeper.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using ZooKeeperNet;

namespace TreeViewLib
{
    public class AirlineReplicationModule : IAirlineReplicationModule, IReplicationModuleEvents
    {
        private static AirlineReplicationModule instance = new AirlineReplicationModule();
        public static AirlineReplicationModule Instance
        {
            get { return instance; }
        }

        private ZooKeeperWrapper zk = null;

        public static readonly string MACHINE_PREFIX = "machine_";
        public static readonly string MACHINES_NODE = "machines";
        public static readonly string SELLERS_NODE = "sellers";
        public static readonly string BARRIER_NODE = "__barrier__";
        public static readonly int SECONDS_TIMEOUT = 10000;
        public static readonly int ZK_RETRIES = 5;

        private string clusterName;
        private string zookeeperAddress;
        private string originalSeller;
        private Uri intraClusterService = null;
        private TreeView tree = null;

        private string id = null;

        public Dictionary<string, ZNodesDataStructures.MachineNode> Machines {
            get { return getMachines(); }
        }

        /// <summary>
        /// Actively queries the zookeeper for this machine's data
        /// </summary>
        public ZNodesDataStructures.MachineNode Machine
        {
            get { return zk.GetData<ZNodesDataStructures.MachineNode>(id, false); }
        }

        public string SellersPath { get { return "/" + clusterName + "/" + SELLERS_NODE; } }
        public string OriginalSellerPath { get { return "/" + clusterName + "/" + SELLERS_NODE + "/" + originalSeller; } }
        public string MachinesPath { get { return "/" + clusterName + "/" + MACHINES_NODE; } }
        public string MachinePath { get { return id;  } }
        public string BarrierPath { get { return "/" + clusterName + "/" + BARRIER_NODE; } }

        public abstract class AirlineReplicationModuleWatcher : IWatcher {
            protected AirlineReplicationModule replicationModule = null;

            public AirlineReplicationModuleWatcher(AirlineReplicationModule repMod)
            {
                replicationModule = repMod;
            }

            abstract public void Process(WatchedEvent @event);
        }

        public class ZooKeeperTreeEvent : AirlineReplicationModuleWatcher
        {
            public ZooKeeperTreeEvent(AirlineReplicationModule airlineReplicationModule) : base(airlineReplicationModule) {}

            public override void Process(WatchedEvent ev)
            {
                replicationModule.treeNodeEvent(ev);
            }        
        }

        public class MachineNodeWatch : AirlineReplicationModuleWatcher
        {
            public MachineNodeWatch(AirlineReplicationModule mod) : base(mod) { }

            public override void Process(WatchedEvent @event)
            {
                replicationModule.machineNodeChanged(@event);
            }
        }

        public class MachinesNodeWatch : AirlineReplicationModuleWatcher
        {
            public MachinesNodeWatch(AirlineReplicationModule mod) : base(mod) { }

            public override void Process(WatchedEvent @event)
            {
                replicationModule.machinesNodeEvent(@event);
            }
        }

        public class SellersNodeWatch : AirlineReplicationModuleWatcher
        {
            public SellersNodeWatch(AirlineReplicationModule mod) : base(mod) { }

            public override void Process(WatchedEvent @event)
            {
                replicationModule.sellersNodeEvent(@event);
            }
        }

        private AirlineReplicationModule() { }

        public AirlineReplicationModule(String address, String clusterName, String originalSeller, Uri localService)
        {
            initialize(address, clusterName, originalSeller, localService);
        }

        public void initialize(String address, String clusterName, String originalSeller, Uri localService)
        {
            this.clusterName = clusterName;
            this.zookeeperAddress = address;
            this.originalSeller = originalSeller;

            if (null == zk)
            {
                zk = new ZooKeeperWrapper(address,SECONDS_TIMEOUT, ZK_RETRIES, new ZooKeeperTreeEvent(this));
            }
            intraClusterService = localService;

            constructClusterSubTree(); // Constructs the cluster's subtree if needed
            registerMachinesNode(); // Add this machine to the Machines subtree
            registerSellersNode();  // Add the seller which this machine represents to the sellers subtree
            
            setMachinesChildrenWatcher();
            setSellersChildrenWatcher();

            tree = new TreeView(zk, MachinesPath, SellersPath);
            tree.refresh();
        }

        public void updateMachineData(ZNodesDataStructures.MachineNode machineData)
        {
            Stat s = zk.SetData(id, machineData);
            if (s == null)
            {
                Console.WriteLine("Update machine data of " + id + " failed");
            }
        }

        private Dictionary<string, ZNodesDataStructures.MachineNode> getMachines()
        {
            Dictionary<string, ZNodesDataStructures.MachineNode> machinesByNames = 
                new Dictionary<string, ZNodesDataStructures.MachineNode>();
            List<String> machines = zk.GetChildren(MachinesPath, false);
            foreach (var machine in machines)
            {
                ZNodesDataStructures.MachineNode data = zk.GetData<ZNodesDataStructures.MachineNode>(MachinesPath + "/" + machine, false);
                machinesByNames.Add(machine, data);
            }
            return machinesByNames;
        }

        /// <summary>
        /// Sets a watcher on the Sellers node, event will call the children changed evend
        /// </summary>
        private void setSellersChildrenWatcher()
        {
            try
            {
                zk.GetChildren(SellersPath, new SellersNodeWatch(this)); // Register creation/deletion of children
            }  catch(Exception ex) 
            {
                Console.WriteLine("Failed setting Sellers Node watch due to " + ex.Message);
                throw ex;
            }
        }

        /// <summary>
        /// Set machines subtree events. this will register the following events:
        /// 1. a new machine node joins the subtree - fires MachinesNodeWatch 
        /// 2. an existing machine got message or died - fires MachineNodeWatch
        /// </summary>
        private void setMachinesChildrenWatcher()
        {
            try
            {
                List<String> machines = zk.GetChildren(MachinesPath, new MachinesNodeWatch(this)); // Watch for new nodes
                foreach(var machine in machines) {
                    String path = MachinesPath + "/" + machine;
                    zk.GetData<ZNodesDataStructures.MachineNode>(path, new MachineNodeWatch(this));
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed setting Machines Node children watch due to " + ex.Message);
                throw ex;
            }                       
        }

        private void setTreeNodeEvent()
        {
            try
            {
                zk.Register(new ZooKeeperTreeEvent(this));
            }
            catch (Exception ex)
            {
                Console.WriteLine("Couldn't set tree watcher because: " + ex.Message);
                throw ex;
            }
        }

        /// <summary>
        /// this function will construct this cluster's subtree if it doesn't exist. cluster's subtree
        /// consists of the parent [cluster] and the children [sellers, nodes].
        /// </summary>
        private void constructClusterSubTree()
        {
            // Construct the cluster's root if it doesn't exist
            try
            {
                Boolean exists = zk.Exists("/" + clusterName);
                if (!exists)
                {
                    zk.Create("/" + clusterName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
                }
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Creation of " + "/" + clusterName  + " failed\n" + e.Message);
                throw e;
            }
            // Construct the child MACHINES_NODE if needed
            try
            {
                Boolean exists = zk.Exists("/" + clusterName + "/" + MACHINES_NODE);
                if (!exists)
                {
                    zk.Create("/" + clusterName + "/" + MACHINES_NODE, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
                }
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Creation of " + "/" + clusterName + "/" + MACHINES_NODE + " failed\n"+e.Message);
                throw e;
            }
            // Construct the child SELLERS_NODE if needed
            try
            {
                Boolean exists = zk.Exists("/" + clusterName + "/" + SELLERS_NODE);
                if (!exists)
                {
                    zk.Create("/" + clusterName + "/" + SELLERS_NODE, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
                }
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Creation of " + "/" + clusterName + "/" + SELLERS_NODE + " failed\n" + e.Message);
                throw e;
            }
        }

        /// <summary>
        /// Initialize this as a new machine - this method will add this to the zookeeper tree
        /// </summary>
        private void registerMachinesNode()
        {
            try
            {
                var node = new ZNodesDataStructures.MachineNode();
                node.uri = intraClusterService;
                node.originalSellerName = originalSeller;
                node.primaryOf.Add(originalSeller);
                byte[] serializedNode = ZNodesDataStructures.serialize(node);
                id = zk.Create(MachinesPath + "/" + MACHINE_PREFIX, serializedNode , Ids.OPEN_ACL_UNSAFE, CreateMode.EphemeralSequential);
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Registering machine node failed\n" + e.Message);
                throw e;
            }            
        }

        private void registerSellersNode()
        {
            try
            {
                if (!zk.Exists(OriginalSellerPath))
                {
                    zk.Create(OriginalSellerPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.Persistent);
                }
            }

            catch (KeeperException e)
            {
                Console.WriteLine("Registering Seller node failed\n" + e.Message);
                throw e;
            }            
        }

        public Dictionary<string, Uri> getSellersAndPrimaries()
        {
            throw new NotImplementedException();
        }

        internal void treeNodeEvent(WatchedEvent @event)
        {
            Console.WriteLine("treeNodeEvent event: " + @event.Type + " on " + @event.Path);

            setTreeNodeEvent();
        }

        internal void machinesNodeEvent(WatchedEvent @event)
        {
            Console.WriteLine("machinesNodeEvent event: " + @event.Type + " on " + @event.Path);
            Console.WriteLine("Event fired on "+ id);
            string[] path = @event.Path.Split('/');
            if ((path.Length >= 3) && (path[1].Equals(clusterName) && (path[2].Equals(MACHINES_NODE))))
            {
                if (@event.Type == EventType.NodeChildrenChanged)
                {
                    TreeView.ChildrenDiff cd = tree.update(); // Get diff
                    foreach (var newNode in cd.added) // Add new watches to new nodes
                    {
                        zk.GetData<ZNodesDataStructures.MachineNode>(MachinesPath + "/" + newNode, new MachineNodeWatch(this));
                    }
                    // Callback function 
                    if (mJoined != null)
                    {
                        foreach (var newNode in cd.added)
                        {
                            int numMachines= zk.GetChildren(MachinesPath, true).Count();
                            ZKBarrier zkb = new ZKBarrier(zk, BarrierPath, numMachines);
                            var machineData = tree.getLocalMachineData(newNode);
                            zkb.Enter();
                            if (mJoined != null)
                            {
                                mJoined(newNode, machineData.originalSellerName, machineData.uri);
                            }
                            zkb.Leave();
                            if (mLBDone != null)
                            {
                                mLBDone();
                            }
                        }
                    }
                }
                else
                {
                    Console.WriteLine("Unexpected event " + @event.Type + " on " + @event.Path + ", Ignoring...");
                }
            }
            else
            {
                Console.WriteLine("Got message on path " + @event.Path + " Which doesn't belong to this TreeView, ignoring");
            }

            zk.GetChildren(MachinesPath, new MachinesNodeWatch(this)); // Refresh watcher on Machines node
        }

        internal void machineNodeChanged(WatchedEvent @event)
        {
            Console.WriteLine("machineNodeDataChanged event: " + @event.Type + " on " + @event.Path);
            Console.WriteLine("Event fired on " + id);
            if (@event.Type == EventType.NodeDataChanged)
            {
                if (@event.Path == id)
                {
                    String machine = @event.Path;
                    machine = machine.Substring(machine.LastIndexOf('/') + 1);
                    ZNodesDataStructures.MachineNode oldData = tree.updateMachineData(machine); // Gets old record and refresh local one
                    ZNodesDataStructures.MachineNode newData = tree.getLocalMachineData(machine); // Gets refreshed record



                    List<String> primariesToLeave   = oldData.primaryOf.Except(newData.primaryOf).ToList();
                    List<String> primariesToJoin    = newData.primaryOf.Except(oldData.primaryOf).ToList();

                    diffMergeMachineSellers(machine, newData.uri, ZNodesDataStructures.SellerNode.NodeRole.Main , primariesToLeave, primariesToJoin);

                    List<String> backupsToLeave = oldData.backsUp.Except(newData.backsUp).ToList();
                    List<String> backupsToJoin = newData.backsUp.Except(oldData.backsUp).ToList();

                    diffMergeMachineSellers(machine, newData.uri, ZNodesDataStructures.SellerNode.NodeRole.Backup, backupsToLeave, backupsToJoin);
                }
            }

            if (@event.Type != EventType.NodeDeleted)
            {
                zk.GetData<ZNodesDataStructures.MachineNode>(@event.Path, new MachineNodeWatch(this));
            }
            else
            {
                if (@event.Type == EventType.NodeDeleted)
                {
                    // 1. Remove this node from tree view
                    // 2. Call the callback method to the leader
                    String machineName = @event.Path.Substring(@event.Path.LastIndexOf('/')+1);
                    ZNodesDataStructures.MachineNode machineData = tree.Machines[machineName];
                    tree.removeMachine(machineName);
                    if (mDropped != null)
                    {
                        mDropped(machineData.primaryOf, machineData.backsUp);
                    }
                }
            }
        }

        private void diffMergeMachineSellers(String machine, Uri machineUri, ZNodesDataStructures.SellerNode.NodeRole machineRole, List<String> toLeave, List<String> toJoin)
        {
            Console.WriteLine("Diff merge " + machineRole + " on " + id);
            foreach (var seller in toJoin)
            {
                ZNodesDataStructures.SellerNode sellerNode = new ZNodesDataStructures.SellerNode() 
                    { role = machineRole, uri = machineUri, nodeId = id };
                zk.Create(SellersPath + "/" + seller + "/" + machine, sellerNode, Ids.OPEN_ACL_UNSAFE, CreateMode.Ephemeral);
            }

            foreach (var seller in toLeave)
            {
                zk.Delete(SellersPath + "/" + seller + "/" + machine);
            }
        }

        internal void sellersNodeEvent(WatchedEvent @event)
        {
            Console.WriteLine("sellersNodeEvent event: " + @event.Type + " on " + @event.Path);
            setSellersChildrenWatcher();
        }


        private VersionRefresh vRefresh = null;
        public VersionRefresh VersionRefreshHandler
        {
            set { vRefresh = value; }
        }

        private MachineJoined mJoined = null;
        public MachineJoined MachineJoinedHandler
        {
            set { mJoined = value; }
        }

        private MachineDropped mDropped = null;
        public MachineDropped MachineDroppedHandler
        {
            set { mDropped = value; }
        }

        public override String ToString()
        {
            return tree.ToString();
        }

        /// <summary>
        ///  Call this function in order to close the ZK connection
        /// </summary>
        public void Dispose()
        {
            zk.Delete(id);
            // Wait for all events to finish, kick the bucket when they are done
            Thread.Sleep(1000);  // I can haz programming
            zk.Disconnect();
        }

        private LoadBalancingDone mLBDone = null;
        public LoadBalancingDone LoadBalancingDoneHandler
        {
            set { mLBDone = value; }
        }
    }
}




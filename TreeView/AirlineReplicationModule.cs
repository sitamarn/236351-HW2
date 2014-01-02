using Org.Apache.Zookeeper.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
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
        public Boolean Connected
        {
            get
            {
                if (zk == null) { return false; } else { return zk.Connected; }
            }
        }


        public static readonly string MACHINE_PREFIX = "machine_";
        public static readonly string MACHINES_NODE = "machines";
        public static readonly string SELLERS_NODE = "sellers";
        public static readonly string BARRIER_NODE = "__barrier__";
        public static readonly int SECONDS_TIMEOUT = 120000;
        public static readonly int ZK_RETRIES = 5;

        private string clusterName;
        private string zookeeperAddress;
        private string originalSeller;
        private Uri intraClusterService = null;
        private TreeView tree = null;

        private string id = "NOT_UP_YET/NOT_UP_YET";

        private Dictionary<string, ZNodesDataStructures.MachineNode> calculatedSnapshot = null;

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
        public string MachineName { get { return id.Substring(id.LastIndexOf('/') + 1); } }

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
         
        public AirlineReplicationModule() { }

        public AirlineReplicationModule(String address, String clusterName, String originalSeller, Uri localService)
        {
            //initialize(address, clusterName, originalSeller, localService);
            throw new NotImplementedException();
        }

        private AutoResetEvent gotName = new AutoResetEvent(false);

        public void waitForNameRegister()
        {
            gotName.WaitOne();
        }

        public void initialize(String address, String clusterName, String originalSeller, Uri localService, MachineJoined mj, MachineDropped md)
        {
            this.mJoined = mj;
            this.mDropped = md;
            this.clusterName = clusterName;
            this.zookeeperAddress = address;
            this.originalSeller = originalSeller;

            if (null == zk)
            {
                zk = new ZooKeeperWrapper(address,SECONDS_TIMEOUT, ZK_RETRIES, new ZooKeeperTreeEvent(this));
            }
            intraClusterService = localService;

            constructClusterSubTree(); // Constructs the cluster's subtree if needed
            //
            tree = new TreeView(zk, MachinesPath, SellersPath);
            //tree.refresh(); // Build tree

            // Set watches
            setMachinesChildrenWatcher();
            setSellersChildrenWatcher();

            registerSellersNode();  // Add the seller which this machine represents to the sellers subtree
            registerMachinesNode(); // Add this machine to the Machines subtree
            //zk.GetData<ZNodesDataStructures.MachineNode>(id, new MachineNodeWatch(this));

            
            //machinesNodeChanged();

            //machinesNodeEvent(WatchedEvent @event);
            
        }

        /// <summary>
        /// Load balancing algorithm is using this function to update the snapshot after its done
        /// </summary>
        /// <param name="snapshot"></param>

        public void updateMachineData(Dictionary<string, ZNodesDataStructures.MachineNode> snapshot)
        {
            Dictionary<String, ZNodesDataStructures.MachineNode> oldMachineData = new Dictionary<string, ZNodesDataStructures.MachineNode>(tree.Machines);
            calculatedSnapshot = snapshot; // TODO: remove this someday, (funny, huh ?)
            // Update Machines local tree
            foreach (var machine in snapshot)
            {
                var currentMachineRecord = machine.Value;
                var previousMachineValue = tree.Machines[machine.Key];

                bool updateZKTree = false;
                if (machine.Key == MachineName)
                {
                    updateZKTree = true;
                }


                if (updateZKTree)
                {
                    zk.SetData(id, currentMachineRecord);
                }
                tree.Machines[machine.Key] = currentMachineRecord;

                var addPrimaries = currentMachineRecord.primaryOf.Except(previousMachineValue.primaryOf);
                var removePrimaries = previousMachineValue.primaryOf.Except(currentMachineRecord.primaryOf);

                var addBackup = currentMachineRecord.backsUp.Except(previousMachineValue.backsUp);
                var removeBackup = previousMachineValue.backsUp.Except(currentMachineRecord.backsUp);

                foreach (var pToAdd in addPrimaries)
                {
                    ZNodesDataStructures.SellerNode primaryRecord = new ZNodesDataStructures.SellerNode()
                    {
                        machineName = machine.Key,
                        role = ZNodesDataStructures.SellerNode.NodeRole.Main,
                        uri = currentMachineRecord.uri,
                        version = -1
                    };
                    tree.SetPrimaryOf(pToAdd, primaryRecord);
                    if (updateZKTree)
                    {
                        zk.Create(SellersPath + "/" + pToAdd + "/" + machine.Key, primaryRecord, Ids.OPEN_ACL_UNSAFE, CreateMode.Ephemeral);
                    }
                }

                foreach (var pToRemove in removePrimaries)
                {
                    tree.RemovePrimaryOf(pToRemove, machine.Key);
                    if (updateZKTree)
                    {
                        zk.Delete(SellersPath + "/" + pToRemove + "/" + machine.Key);
                    }
                }

                foreach (var bToAdd in addBackup)
                {
                    ZNodesDataStructures.SellerNode primaryRecord = new ZNodesDataStructures.SellerNode()
                    {
                        machineName = machine.Key,
                        role = ZNodesDataStructures.SellerNode.NodeRole.Backup,
                        uri = currentMachineRecord.uri,
                        version = -1
                    };
                    tree.SetBackupOf(bToAdd, primaryRecord);
                    if (updateZKTree)
                    {
                        zk.Create(SellersPath + "/" + bToAdd + "/" + machine.Key, primaryRecord, Ids.OPEN_ACL_UNSAFE, CreateMode.Ephemeral);
                    }
                }

                foreach (var bToRemove in removeBackup)
                {
                    tree.RemoveBackupOf(bToRemove, machine.Key);
                    if (updateZKTree)
                    {
                        zk.Delete(SellersPath + "/" + bToRemove + "/" + machine.Key);
                    }
                }                


                //tree.Machines[machine.Key] = machine.Value;
            }
            // STOP HERE
        }

        private Dictionary<string, ZNodesDataStructures.MachineNode> getMachines()
        {
            Dictionary<string, ZNodesDataStructures.MachineNode> machinesByNames =
                new Dictionary<string, ZNodesDataStructures.MachineNode>(tree.Machines);
            return machinesByNames;
        }

        /// <summary>
        /// Sets a watcher on the Sellers node, event will call the children changed evend
        /// </summary>
        private void setSellersChildrenWatcher()
        {
            if (zk.Connected)
            {
                try
                {
                    zk.GetChildren(SellersPath, new SellersNodeWatch(this)); // Register creation/deletion of children
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Failed setting Sellers Node watch due to " + ex.Message);
                    throw ex;
                }
            }
        }

        /// <summary>
        /// Set machines subtree events. this will register the following events:
        /// 
        /// <para>1. a new machine node joins the subtree - fires MachinesNodeWatch</para>
        /// <para>2. an existing machine got message or died - fires MachineNodeWatch</para>
        /// 
        /// </summary>
        /// 
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
                if(zk.Connected)
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
        /// Create a node under /cluster/MACHINES for this
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
                gotName.Set();
            }
            catch (KeeperException e)
            {
                Console.WriteLine("Registering machine node failed\n" + e.Message);
                throw e;
            }            
        }

        /// <summary>
        /// Creates a seller node under /cluster/SELLERS for the seller which came up with this machine
        /// if seller already exist, do nothing
        /// </summary>
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
            Console.WriteLine("["+MachineName+"] treeNodeEvent event: " + @event.Type + " on " + @event.Path);
            if ((@event.Type == EventType.None) && (@event.State == KeeperState.Disconnected))
            {
                moduleDisconnect();
            }
            else
            {
                setTreeNodeEvent();
            }
        }

        internal void machinesNodeEvent(WatchedEvent @event)
        {

            Console.WriteLine("[" + MachineName + "] machinesNodeEvent " + @event.Type + " on " + @event.Path );
            //Console.WriteLine("[" + MachineName + "] machinesNodeEvent " + !((@event.Type == EventType.None) && (@event.Path == id)));
            if (!((@event.Type == EventType.None)&&(@event.Path==id))) // NOT a deletion event
            {
                string[] path = @event.Path.Split('/');
                if ((path.Length >= 3) && (path[1].Equals(clusterName) && (path[2].Equals(MACHINES_NODE))))
                {
                    if (@event.Type == EventType.NodeChildrenChanged) // Somebody Joined/Left 
                    {
                        machinesNodeChanged();
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

                if (zk.Connected) // Reset event handler
                {
                    zk.GetChildren(MachinesPath, new MachinesNodeWatch(this)); // Refresh watcher on Machines node
                }
            }
        }

        private void machinesNodeChanged()
        {
            if (!Connected)
            {
                Console.WriteLine("[" + MachineName + "] Ignoring machinesNodeChanged because connection dropped");
                return;
            }
            List<String> machines = zk.GetChildren(MachinesPath, false);
            var machinesDropped = tree.Machines.Keys.Except(machines);
            var machinesJoined = machines.Except(tree.Machines.Keys);

            if ((machinesJoined.Count() + machinesDropped.Count()) > 2)
            {
                Console.WriteLine("["+MachineName+"] *WARNING* - more changes than expected, ignoring all but first:");
                Console.WriteLine("["+MachineName+"] *WARNING* - joined  : " + String.Join(" ",machinesJoined.ToArray()));
                Console.WriteLine("["+MachineName+"] *WARNING* - dropped : " + String.Join(" ", machinesJoined.ToArray()));
            }

            if (!machinesJoined.IsEmpty())
            {
                // 1. Add new nodes to tree
                // 2. Run callback if exists
                String joined = machinesJoined.First();
                ZNodesDataStructures.MachineNode joinedData = zk.GetData<ZNodesDataStructures.MachineNode>(MachinesPath + "/" + joined, new MachineNodeWatch(this)); // Set watch on new node
                tree.addMachine(joined, joinedData);

                if (null != mJoined)
                {
                    printRemoteAndLocalTree("PRE-CALLBACKS");
                    mJoined(joined, tree.Machines[joined].originalSellerName, tree.Machines[joined].uri);
                    printRemoteAndLocalTree("POST-CALLBACKS");
                }
            }
            if (!machinesDropped.IsEmpty())
            {
                Console.WriteLine("["+MachineName+"] Machine dropped - " + String.Join(" ", machinesDropped));
                // - Dropped clients should be dealt exclusively by the machineNodeChange callback and not here

                // 1. Remove dropped machine from tree
                // 2. Run callback if exists                
                //String dropped = machinesDropped.First();
                //ZNodesDataStructures.MachineNode droppedData = tree.Machines[dropped];
                //tree.removeMachine(dropped);

                //if (null != mDropped)
                //{
                //    mDropped(droppedData.primaryOf, droppedData.backsUp);
                //}
            }

            

        }

        private void printRemoteAndLocalTree(String reason)
        {
            Console.WriteLine("[" + MachineName + "] "+reason+" showing ZK Tree");
            Console.WriteLine(TreeView.PrintTree(zk, MachinesPath, SellersPath, MachineName));
            Console.WriteLine("[" + MachineName + "] "+reason+" showing Local Tree:");
            Console.WriteLine(tree.ToString());
        }

        internal void machineNodeChanged(WatchedEvent @event)
        {
            Console.WriteLine("["+MachineName+"] machineNodeDataChanged event: " + @event.Type + " on " + @event.Path);
            Console.WriteLine("["+MachineName+"] Event fired on " + id);
            if (@event.Type == EventType.NodeDataChanged)
            {
                if (@event.Path == id)
                {
                    Console.WriteLine("["+MachineName+"] Got update message to my own machine node - ignoring...");
                }
            }

            if (@event.State != KeeperState.Disconnected)
            {
                if (@event.Type != EventType.NodeDeleted) // If it wasn't node deletion - don't re-add listener to dead machine nodes
                {
                    zk.GetData<ZNodesDataStructures.MachineNode>(@event.Path, new MachineNodeWatch(this));
                }
                else 
                {
                    if (@event.Type == EventType.NodeDeleted) // Some machine was deleted
                    {
                        if (@event.Path != id) // Deleted machine is not me
                        {
                            String machineName = @event.Path.Substring(@event.Path.LastIndexOf('/') + 1);
                            ZNodesDataStructures.MachineNode machineData = tree.Machines[machineName];
                            Console.WriteLine("[" + MachineName + "] MachineData of " + machineName + ": ");
                            Console.WriteLine(machineData.ToString());
                            tree.removeMachine(machineName);
                            if (mDropped != null)
                            {
                                printRemoteAndLocalTree("PRE-DROP-CALLBACK");

                                Console.WriteLine("["+MachineName+"] Calling machine dropped callback");
                                mDropped(machineData.primaryOf, machineData.backsUp);

                                printRemoteAndLocalTree("POST-DROP-CALLBACK");
                            }
                        }
                    }
                }
            }
        }

        private void moduleDisconnect()
        {
            Console.WriteLine("[" + MachineName + "] MODULE DISCONNECT CALLBACK");
        }

        /// <summary>
        /// This function will update a machine in the Sellers subtree according to the parameters it receives
        /// <para>TODO: if machine is set as backup, it should listen to primary and replace it upon failure</para>
        /// </summary>
        /// <param name="machine">The name of the machine to add in the added seller's subtree</param>
        /// <param name="machineUri">Uri of the machine to add</param>
        /// <param name="machineRole">The role to add this machine. Backup or Primary</param>
        /// <param name="toLeave">Sellers which this machine need to leave</param>
        /// <param name="toJoin">Sellers which this machine need to join</param>
        private void diffMergeMachineSellers(String machine, Uri machineUri, ZNodesDataStructures.SellerNode.NodeRole machineRole, List<String> toLeave, List<String> toJoin)
        {
            throw new NotImplementedException("FUCK YOU");
        }

        internal void sellersNodeEvent(WatchedEvent @event)
        {
            Console.WriteLine("[" + MachineName + "] sellersNodeEvent event: " + @event.Type + " on " + @event.Path);
            setSellersChildrenWatcher();
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

        public ZKBarrier barrier() 
        {
            int machines = zk.GetChildren(MachinesPath, false).Count();
            ZKBarrier barrier = ZKBarrier.Barrier(zk, BarrierPath, MachineName, machines); // Create and block barrier
            return barrier;
        }

        public void testDie()
        {
            zk.Disconnect();
            Console.WriteLine("["+MachineName+"] TestDie - Killed connection, blocking for 5 seconds");
            Thread.Sleep(5 * 1000);
            Console.WriteLine("Done");
        }
    }
}



